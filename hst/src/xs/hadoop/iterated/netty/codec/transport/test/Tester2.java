package xs.hadoop.iterated.netty.codec.transport.test;

import java.io.FileInputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.IOUtils;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import xs.hadoop.iterated.netty.codec.transport.Listen;
import xs.hadoop.iterated.netty.codec.transport.ListenFactory;
import xs.hadoop.iterated.netty.codec.transport.ReceiverHandler;
import xs.hadoop.iterated.netty.codec.transport.SenderHandler;


/**
 * 真正的测试需要写一个proxy,模拟网络中的种种情况,比如将消息"吃回扣",不发给另一端 
 */
public class Tester2 {

	
	ClientBootstrap  clientBootstrap;
	
	public Channel newClientChannel(){
		ChannelFuture future = clientBootstrap.connect(new InetSocketAddress("localhost", 29001));
		future.awaitUninterruptibly();
		
		return future.getChannel();
	}
	
	public void testClient(){
		clientBootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                Executors.newSingleThreadExecutor(),
                Executors.newSingleThreadExecutor()));
		
		clientBootstrap.setPipelineFactory(SenderHandler.createCenderChannelPipelineFactory());
		
		
		Channel channel = newClientChannel();
		Exception[] inout = new Exception[1];
		byte[] message = "some message".getBytes();
		
		long start = System.currentTimeMillis();
		int messageId = SenderHandler.sendMessage(channel, message, inout, null);
		System.out.println("发送首条消息用时:"+(System.currentTimeMillis()-start));
		
		if(inout[0]!=null){
			inout[0].printStackTrace();
			channel.close();
			channel = newClientChannel();
			messageId = SenderHandler.sendMessage(channel, message, inout, messageId);
		}
		
		try {
			FileInputStream fis = new FileInputStream("C:/Users/Administrator/Desktop/jQuery-1.8.chm");
			byte[] array = IOUtils.toByteArray(fis);
			System.out.println(array.length);
			
			start = System.currentTimeMillis();
			messageId = SenderHandler.sendMessage(channel, array, inout, null);
			System.out.println("发送消息用时:"+(System.currentTimeMillis()-start));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	ServerBootstrap bootstrap;
	
	public void testServer(){
		bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));
		
		
		class TestListen extends LinkedBlockingQueue<byte[]> implements Listen{
			public void put(String clientIp,int messageId, byte[] message)
					throws InterruptedException {
				super.put(message);
			}
		}
		
		final ConcurrentHashMap<String,TestListen> map = new ConcurrentHashMap();
		
		bootstrap.setPipelineFactory(ReceiverHandler.createReceiverChannelPipelineFactory(new ListenFactory() {
			public Listen getListen(String clientId,byte[] message) {
				Listen listen = map.get(clientId);
				if(listen==null){
					map.putIfAbsent(clientId, new TestListen());
					listen = map.get(clientId);
				}
				return listen;
			}
		}));
		
		
		
		/**
		 * 消息的消费比较麻烦,需要自己做线程调度
		 */
		new Thread(){
			public void run() {
				while(true){
					try {
						for(TestListen queue:map.values().toArray(new TestListen[map.size()])){
							byte[] message;
							while((message = queue.poll())!=null){
								System.out.println("----"+message.length);
							}
						}
						Thread.sleep(100);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}.start();
		
		bootstrap.bind(new InetSocketAddress(29001));
	}
	
	
	public static void main(String[] args) {
		try {
			Tester2 tt = new Tester2();
			tt.testServer();
			tt.testClient();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	

}
