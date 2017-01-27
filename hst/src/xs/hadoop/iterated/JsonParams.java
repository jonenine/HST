package xs.hadoop.iterated;

import xs.hadoop.Json;


/**
 * 可以通过json配置的基础接口
 */
public interface JsonParams {
	
	public void setJsonStr(String jsonStr);
	
	public String getJsonStr();
	
	public Json getJson();
}
