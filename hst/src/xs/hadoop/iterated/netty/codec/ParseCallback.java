package xs.hadoop.iterated.netty.codec;

import java.util.Map;

public interface ParseCallback {
	/**
	 * 当解析完一个frame的时候会调 
	 */
	public abstract void onFrameClosed(Map<String, Object> itemsSoFar);
	
	public abstract void onHasSomethingWrong(byte[] errorByte, String string);
}

