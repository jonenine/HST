package xs.hadoop.iterated.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import xs.hadoop.iterated.netty.codec.transport.ReceiverHandler;

/**
 */
public class MessageServer extends JsonListenFactory{

	ServerBootstrap bootstrap;
	
	public void bind(int port) throws Exception {
		if (bootstrap != null) 	throw new Exception("server already start");
		
		/**
		 * 只允许监听一个端口
		 */
		bootstrap  = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newFixedThreadPool(3),Executors.newCachedThreadPool()));
		
		bootstrap.setPipelineFactory(ReceiverHandler.createReceiverChannelPipelineFactory(this));
		
		bootstrap.setOption("child.tcpNoDelay", true);
		bootstrap.setOption("child.keepAlive", false);
		bootstrap.setOption("child.receiveBufferSize", 1048576*20);
		
		bootstrap.bind(new InetSocketAddress(port));
	}
	
	
	public void close(){
		if(bootstrap!=null) bootstrap.releaseExternalResources();
		bootstrap = null;
	}


}



