package xs.hadoop.iterated.netty.codec;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;

import xs.hadoop.Json;


public class FrameParseDecoder extends OneToOneDecoder implements ParseCallback{
	
	private Parser parser;
	
	public FrameParseDecoder(String protocolName) {
		parser = Protocols.getProtocols().createParser(protocolName, this);
	}
	
	boolean setCloseFurture=false;
	
	/**
	 */
	protected Object decode(ChannelHandlerContext ctx, Channel channel,Object msg) throws Exception {
		
		//在channel关闭的时候,执行parser.stop(),使得最后一部分无用数据可以做日志
		if(!setCloseFurture){
			ctx.getChannel().getCloseFuture().addListener(new ChannelFutureListener(){
				public void operationComplete(ChannelFuture future) throws Exception {
					parser.stop();
				}
			});
			setCloseFurture=true;
		}
		
		//得到这次返回的数据
		ChannelBuffer cb = (ChannelBuffer)msg;
		byte[] array = new byte[cb.readableBytes()];
		cb.readBytes(array);
		//解析,如果有结果就让这个消息流转
		parser.parse(array);
		if(!items.isEmpty()){
			Map<String, Object> re = null;
			for(Map<String, Object> item:items){
				//这里需要验证一下,就只有一个值
				re = item;
			}
			items.clear();
			return re;
		}
		
		return null;
	}

	private List<Map<String, Object>>  items= new LinkedList<Map<String,Object>>();
	public void onFrameClosed(Map<String, Object> itemsSoFar) {
		items.add(itemsSoFar);
	}
	
	/**
	 * 做日志
	 */
	public void onHasSomethingWrong(byte[] errorByte, String string) {
		System.err.println("解析错误 段"+string+","+new Json(errorByte));
	}

}


