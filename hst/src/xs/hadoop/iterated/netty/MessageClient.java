package xs.hadoop.iterated.netty;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import xs.hadoop.iterated.netty.codec.transport.SenderHandler;

/**
 * 这个客户端会长时间保持链接,不在这里使用,代码保持在这里
 * 另外一个,升级到netty4可能会更成熟一些
 */
public class MessageClient {
	
	static ClientBootstrap  clientBootstrap;
	
	public static Channel newClientChannel(String host,int port){
		ChannelFuture future = clientBootstrap.connect(new InetSocketAddress(host, port));
		future.awaitUninterruptibly();
		
		return future.getChannel();
	}
	
	
	static Map<String, Channel> conntectedMap = new ConcurrentHashMap();
	
	/**
	 * 这个方法不是线程安全的,这能自己单独串行使用
	 * 发完一个消息,连接必须断掉,否则会在服务器端占用一个线程
	 */
	private static void sendMessageSync(String host,int port,byte[] message) throws Exception{
		if(clientBootstrap == null) {
			 clientBootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                    Executors.newSingleThreadExecutor(),
                    Executors.newSingleThreadExecutor()));
		
			 clientBootstrap.setPipelineFactory(SenderHandler.createCenderChannelPipelineFactory());
			 
			 //注意客户端的配置(同服务端是不一样的),否则tcp会Delay,40毫秒才发出
			 clientBootstrap.setOption("tcpNoDelay", true);
			 clientBootstrap.setOption("keepAlive", false);
		} 
		
		
		Exception[] inout = null;
		Integer messageId = null;
		int i=1;
		
		final String key = host + ":" + port;
		Channel channel = conntectedMap.get(key);
		
		do{
			//如果不是第一次执行(出现错误了)
			if(inout!=null && inout[0]!=null)  {
				channel.close().awaitUninterruptibly();
				channel = null;
			}
			
			if(channel==null){
				//重新连一个新的
				channel = newClientChannel(host, port);
				channel.getCloseFuture().addListener(new ChannelFutureListener() {
					public void operationComplete(ChannelFuture arg0) throws Exception {
						conntectedMap.remove(key);
					}
				});
				
				//等待旧的channel remove掉,估计没必要,还是放在这吧
				while(conntectedMap.get(key)!=null) Thread.sleep(1);
				//新的channel加入conntectedMap
				conntectedMap.put(key, channel);
			}
			
			inout = new Exception[1];
			messageId = SenderHandler.sendMessage(channel, message, inout, messageId);
			
			//没什么意义
			Thread.sleep(50*(i-1));
		}while(inout[0]!=null && i++<3);
		if(inout[0]!=null) throw inout[0];
	}
	
	/**
	 *  等待消息都发送完成,再停止
	 */
	public synchronized static void close(){
		if(asyncThread.isAlive() && !asyncThreadEnd.get()){
			try {
				asyncSendQueue.put(endFlag);
				while(!asyncThreadEnd.get()) Thread.sleep(15);
				asyncThread.interrupt();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		if(clientBootstrap!=null){
			for(Channel channel:conntectedMap.values()){
				channel.close().awaitUninterruptibly();
			}
			//经过测试,线程池一起销毁了
			clientBootstrap.releaseExternalResources();
			clientBootstrap = null;
		}
		
	}
	
	/*---------------异步--------------*/
	
	static LinkedBlockingQueue<Object[]> asyncSendQueue = new LinkedBlockingQueue<Object[]>();
	
	final static Object[] endFlag = new Object[0];
	
	static AtomicBoolean asyncThreadEnd = new AtomicBoolean(false);
	
	static Thread asyncThread = new Thread(){
		public void run() {
			while(!this.isInterrupted()){
				try {
					Object[] params = asyncSendQueue.take();
					if(params == endFlag) {
						asyncThreadEnd.set(true);
						break;
					}
					String host = (String) params[0];
					int port    = (Integer) params[1];
					byte[] message = (byte[]) params[2];
					sendMessageSync(host, port, message);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	};
	
	
	static AtomicBoolean isAsyncThreadStart = new AtomicBoolean(false);
	/**
	 * 容易出现的问题是,进程推出的时候,还有没发送完的消息 
	 */
	public static void sendMessageAsync(String host, int port, byte[] message){
		try {
			asyncSendQueue.put(new Object[]{host,port,message});
			if(isAsyncThreadStart.compareAndSet(false, true)) asyncThread.start();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
   
}










