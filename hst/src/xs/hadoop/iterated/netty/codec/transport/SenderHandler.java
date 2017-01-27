package xs.hadoop.iterated.netty.codec.transport;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.Adler32;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;


/**
 * 在decoder之后,会到这里完成从规约层到传输层的传递 
 */
public class SenderHandler extends SimpleChannelUpstreamHandler {
	
	/**
	 * 握手响应超时时间
	 */
	long replyOvertime = 500;
	
	//内部重试次数,也就是在不断连接的情况下重试的次数
	int repeatTime = 3;
	
	
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)	throws Exception {
		//处理接收端握手回应
		ChannelBuffer buffer = (ChannelBuffer)e.getMessage();
		int success = buffer.readByte() & 0xff;
		
		if(success==ReceiverHandler.success){
			int messageInt = buffer.readInt();
			try {
				sendMap.get(messageInt).put((int)success);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
	}
	
	private static int messageId = 0;
	
	/**
	 * messageId是循环使用的,而且是所有业务共享的,int也应该足够大,防止瞬间转一圈的id重复现象
	 */
	private synchronized static int getNextMessageId(){
		int nextId = messageId++;
		if(nextId == Integer.MAX_VALUE) {
			messageId = 0;
			nextId = messageId++;
		}
		return nextId;
	}
	
	private static final int maxLength = (int)Short.MAX_VALUE;
	/**
	 * 此方法没加同步,不是线程安全的,所有的场景也都是串行调用
	 */
	public int sendMessageInternal(Channel channel,byte[] overload,Exception[] intout,Integer repeatMessageId){
		
		int nextMessageId = repeatMessageId==null?getNextMessageId():repeatMessageId;
		
		Integer returnMessageId = null;
		
		int overloadLength = overload.length;
		if(overloadLength > maxLength){
			int frameIndex = 0;
			int frameNums = overloadLength/maxLength;
			frameNums = frameNums*maxLength<overloadLength?frameNums+1:frameNums;
			
			
			int frameSize = maxLength;
			do{
				byte[] _overload = new byte[frameSize];
				System.arraycopy(overload, frameIndex*frameSize, _overload, 0, frameSize);
				
				//多帧消息的消息号按照第一帧
				if(returnMessageId == null) returnMessageId = nextMessageId;
				else  nextMessageId = getNextMessageId();
				try {
					sendMessageRepeat(channel,nextMessageId,frameNums,frameIndex,_overload);
				} catch (Exception e) {
					intout[0] = e;
					break;
				}
				
				frameIndex++;
				
				//最后一波处理
				overloadLength = overloadLength-maxLength;
				frameSize = (overloadLength>maxLength)?maxLength:overloadLength;
			}while(overloadLength>0);
			
		}else{
			returnMessageId = nextMessageId;
			try {
				sendMessageRepeat(channel,nextMessageId,1,0,overload);
			} catch (Exception e) {
				intout[0] = e;
			}
		}
		
		return returnMessageId;
	}
	
	/**
	 * 封装错误处理 
	 */
	public void sendMessageRepeat(Channel channel,int messageId,int frameNums,int frameIndex,byte[] overload) throws Exception{
		Integer reply = null;
		int intenalRepeat = 0;
		do{
			reply = sendMessage(channel, messageId,frameNums, frameIndex, overload);
		}while(++intenalRepeat<repeatTime && (reply==null || reply.intValue() != ReceiverHandler.success));
		
		try{
			if(intenalRepeat>1) throw new RuntimeException("can't send message in "+repeatTime+" times");
		}finally{
			sendMap.remove(messageId);
		}
	}
	
	ConcurrentHashMap<Integer, BlockingQueue<Integer>> sendMap = new ConcurrentHashMap();
	
	/**
	 * 返回null,就是没有及时响应 
	 */
	public Integer sendMessage(Channel channel,int messageId,int frameNums,int frameIndex,byte[] overload) throws InterruptedException{
		
		ChannelBuffer cb = ChannelBuffers.buffer(11);
		cb.writeByte(0XA1);
		cb.writeInt(messageId);
		cb.writeShort(frameNums);
		cb.writeShort(frameIndex);
		cb.writeShort(overload.length);
		channel.write(cb);
		
		cb = ChannelBuffers.buffer(overload.length);
		cb.writeBytes(overload);
		channel.write(cb);
		
		Adler32 checksum = new Adler32();
		checksum.update(overload);
		long check = checksum.getValue();
		
		cb = ChannelBuffers.buffer(8);
		cb.writeLong(check);
		channel.write(cb);
		
		cb = ChannelBuffers.buffer(1);
		cb.writeByte(0XA2);
		
		//在最后一个字节发送之前
		BlockingQueue<Integer> queue;
		//如果这个消息是第一次发送
		if((queue = sendMap.get(messageId))==null){
			sendMap.putIfAbsent(messageId, new LinkedBlockingQueue<Integer>());
			queue = sendMap.get(messageId);
		}
		
		channel.write(cb);
		
		//如果同样的消息,以前重试的握手响应回来了,也算成功.注意这里会阻塞,一般是业务逻辑主线程,正好和netty worker线程并发
		return queue.poll(replyOvertime, TimeUnit.MILLISECONDS);
	}
	
	
	public void exceptionCaught(ChannelHandlerContext arg0, ExceptionEvent arg1) throws Exception {
		super.exceptionCaught(arg0, arg1);
		arg1.getCause().printStackTrace();
		arg0.getChannel().close();
	}
	
	/**
	 * @messageId 具有唯一性,如果指定,就按照这个messageId发送,用于断连接之后重发
	 * @return 返回消息号 
	 */
	public static int sendMessage(Channel channel,byte[] overload,Exception[] inout,Integer repeatMessageId){
		SenderHandler sender = (SenderHandler) channel.getPipeline().get("sender");
		
		int returnMessageId = sender.sendMessageInternal(channel,overload,inout,repeatMessageId);
		
		return returnMessageId;
	}
	
	/**
	 * 创建消息发送方的pipeline 
	 */
	public static ChannelPipelineFactory createCenderChannelPipelineFactory(){
		return new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
            	ChannelPipeline p = new DefaultChannelPipeline();
               
            	p.addLast("sender", new SenderHandler());
            	
            	return  p;
            }
		};
	
	}
	
	
}
