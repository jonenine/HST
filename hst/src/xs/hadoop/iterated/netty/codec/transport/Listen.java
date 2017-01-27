package xs.hadoop.iterated.netty.codec.transport;


/**
 *
 */
public interface Listen {
	

	/**
	 *  消息号具有唯一性,在传输层无法阻止一条消息多次发送(at least once),只好在这里判断一下
	 *  注意很有可能这个方法是多线程调用的,所以要注意同步的问题
	 */
	public void put(String clientIp,int messageId,byte[] message) throws InterruptedException ;
	
	
}


