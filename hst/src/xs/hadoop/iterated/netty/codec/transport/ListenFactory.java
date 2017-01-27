package xs.hadoop.iterated.netty.codec.transport;



public interface ListenFactory {
	
	/**
	 * 或创建,或从cache中取得 
	 */
	public Listen getListen(String clientIp,byte[] message);
}
