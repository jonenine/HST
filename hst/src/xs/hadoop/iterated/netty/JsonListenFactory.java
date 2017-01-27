package xs.hadoop.iterated.netty;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import xs.hadoop.Json;
import xs.hadoop.iterated.netty.codec.transport.Listen;
import xs.hadoop.iterated.netty.codec.transport.ListenFactory;


/**
 * 1.将message转化为json
 * 2.通过json中的url来区分业务,并提供业务的注册和注销 
 */
public class JsonListenFactory implements ListenFactory {
	
	public static abstract class JsonListen implements Listen{

		public void put(String clientIp, int messageId, byte[] message)
				throws InterruptedException {
			String jsonStr;
			try {
				jsonStr = new String(message,"UTF-8");
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
			Json json = Json.desierialize(jsonStr);
			put(clientIp,messageId,json);
		}
		
		public abstract void put(String clientIp, int messageId, Json json) throws InterruptedException ;
		
	} 

	public Listen getListen(String clientIp, byte[] message) {
		try {
			String jsonStr = new String(message,"UTF-8");
			Json json = Json.desierialize(jsonStr);
			
			Json urlJson = json.find("url");
			if(urlJson!=null){
				String url = qulifiedUrl(urlJson.toString());
				Listen listen = ListenMap.get(url);
				if(listen!=null) return listen;
			}
			
			throw new RuntimeException("can't find registed url from jsonMessage:"+jsonStr);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 规范url,使得一些拼加的url可以起作用 
	 * 必须以/开头,结尾不能有/
	 */
	private static String qulifiedUrl(String url){
		String qulifiedUrl = url.replace("\\", "/").replaceAll("/+", "/");
		if(!qulifiedUrl.startsWith("/")) qulifiedUrl = "/"+qulifiedUrl;
		if(qulifiedUrl.endsWith("/")) qulifiedUrl = qulifiedUrl.substring(0, qulifiedUrl.length()-1);
		
		return qulifiedUrl;
	}
	
	
	private Map<String, JsonListen> ListenMap = new ConcurrentHashMap();
	
	public void regist(String url,JsonListen listen){
		ListenMap.put(qulifiedUrl(url), listen);
	}
	
	public void unRegist(String url){
		ListenMap.remove(qulifiedUrl(url));
	}
}


