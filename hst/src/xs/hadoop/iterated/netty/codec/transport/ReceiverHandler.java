package xs.hadoop.iterated.netty.codec.transport;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

import xs.hadoop.iterated.netty.codec.FrameParseDecoder;
import xs.hadoop.iterated.netty.codec.FunctionTemplate;
import xs.hadoop.iterated.netty.codec.Parser;
import xs.hadoop.iterated.netty.codec.Protocols;

/**
 * 应该按照监听的端口分组
 */
public class ReceiverHandler extends SimpleChannelUpstreamHandler{
	
	static {
		Protocols protocols= Protocols.getProtocols();
		//设置规约
		protocols.setFrameConfigs(new HashMap<String, String>(){{
			put("default", "/xs/hadoop/iterated/netty/codec/transport/protocol.js");
		}});
		
		//设置function
		protocols
			.addFunction("default", "int", new FunctionTemplate(){
				public Object out(Parser parser) {
					ChannelBuffer cb = ChannelBuffers.copiedBuffer(parser.getCurrentItemArray());
					switch (cb.readableBytes()){
						case 4: return cb.readInt();
						case 2: return (int)cb.readShort();
						case 1: return (int)cb.readByte();
						default: throw new RuntimeException("can't read to int from ChannelBuffer:"+cb);
					}
				}
			}).addFunction("default", "frameLength", new FunctionTemplate(){
				public int length(Parser parser) {
					return (Integer)parser.getItemsSoFar().get("frameLength");
				}
			}).addFunction("default", "byte", new FunctionTemplate(){
				public Object out(Parser parser) {
					return parser.getCurrentItemArray();
				}
			}).addFunction("default", "adler32", new FunctionTemplate(){
				public boolean validate(Parser parser) {
					byte[] overload = (byte[]) parser.getItemsSoFar().get("overload");
					
					Adler32 checksum = new Adler32();
					checksum.update(overload);
					long check = checksum.getValue();
					
					//利用netty默认的机制进行转换
					ChannelBuffer cb = ChannelBuffers.copiedBuffer(parser.getCurrentItemArray());
					if(check == cb.readLong()) return true;
					else return false;
				}
			});
		
	}
	
	public static final int success = 0xF1;
	
	//一旦报错,都是无法响应的,引为可能是由报文头及报文尾判断不准确造成的虚报,所以这个应该不用
	public static final int fail = 0xF2;
	
	/**
	 *  响应成功给发送端
	 */
	void replySuccess(Channel channel,int id){
		ChannelBuffer cb = ChannelBuffers.dynamicBuffer();
		
		//只要有一次发送成功就可以了,不计较重复次数,这里无疑也延长了超时时间
		cb.writeByte(success);
		cb.writeInt(id);
		
		channel.write(cb);
	}
	
	int alreadyMessageId = -1;
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)	throws Exception {
		if (!(e.getMessage() instanceof Map)) return;
		Map<String, Object> messages = (Map<String, Object>) e.getMessage();
		
		if(messages!=null){
			int id = (Integer) messages.get("messageId");
			
			//防止发重,在连接切换的时候仍有可能发重
			if(id != alreadyMessageId){
				alreadyMessageId = id;
				replySuccess(ctx.getChannel(),id);
				
				//取得客户端id,目前是取得客户端ip
				InetSocketAddress address = (InetSocketAddress) ctx.getChannel().getRemoteAddress();
				String clientIp = address.getAddress().getHostAddress();
				
				processToListen(messages,clientIp);
			}
			
			/**
			 * 要不发送以前的信息(握手响应没有及时返回,导致发送端又重新发送了一次)
			 * 要不发送未来的消息(这是不可能发生的,没有握手响应确认,发送端不会做这样的事情)
			 */
		}
	}
	
	
	/**
	 * 以下三个状态还没能去掉,意味着多帧的消息是不能断连接重发的
	 * 可以把这部分业务放到listen中去
	 */
	private List<byte[]> multiMessage;

	private int multiMessageId = -1;
	private int messageLength  = 0;
	
	private void processToListen(Map<String, Object> messages,String clientIp){
		byte[] overload = (byte[]) messages.get("overload");
		
		//对非单帧的信息进行组帧
		int frameNums  = (Integer) messages.get("frameNums");
		int frameIndex = (Integer) messages.get("frameIndex");
		if(frameNums>1){
			if(frameIndex == 0) {
				multiMessage   = new ArrayList<byte[]>(frameNums);
				//多帧消息的消息号按照第一帧
				multiMessageId = (Integer) messages.get("messageId");
				messageLength  = 0;
			}
			
			if(multiMessage!=null) multiMessage.add(overload);
			messageLength+=overload.length;
					
			if(frameIndex == frameNums-1 && multiMessage!=null){
				overload = new byte[messageLength];
				int i=0;
				for(byte[] _overload:multiMessage){
					System.arraycopy(_overload, 0, overload, i, _overload.length);
					i+=_overload.length;
				}
				//clear
				multiMessage  = null;
				messageLength = 0;
			}else{
				return;
			}
		}
				
		//注意ListenFactory是一个静态变量,所以getListen被使所有接受端业务共享,所以这个方法一定是个同步方法
		Listen listen = ListenFactory.getListen(clientIp, overload);
		
		try {
			//这里不应该同步处理,因为业务代码可能会阻塞netty的worker线程,这里简单的放入listen的缓存中,至于listen如何消费,那不是这个代码要关心的事情
			int messageId = (Integer) messages.get("messageId");
			listen.put(clientIp,(multiMessageId==-1?messageId:multiMessageId),overload);
			
			//clear,不要忘记
			if(multiMessageId!=-1) multiMessageId = -1;
		} catch (InterruptedException e) {}
	}
	
	
	public void exceptionCaught(ChannelHandlerContext arg0, ExceptionEvent arg1) throws Exception {
		super.exceptionCaught(arg0, arg1);
		arg1.getCause().printStackTrace();
		arg0.getChannel().close();
	}

	/**
	 * 创建消息接收方的pipeline
	 */
	public static ChannelPipelineFactory createReceiverChannelPipelineFactory(final ListenFactory listenFactory) {
		return new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
            	ChannelPipeline p = new DefaultChannelPipeline();
               
            	//1.数据先到这里解码
            	p.addLast("encoder", new FrameParseDecoder("default"));
            	//2.(1)完成握手 (2)推送给业务层
            	p.addLast("receiver", new ReceiverHandler(listenFactory));
            	
            	return  p;
            }
		};
	
	}
	
	
	ListenFactory ListenFactory;
	
	
	
	public ReceiverHandler(ListenFactory listenFactory) {
		super();
		ListenFactory = listenFactory;
	}



}



