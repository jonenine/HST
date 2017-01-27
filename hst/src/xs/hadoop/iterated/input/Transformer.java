package xs.hadoop.iterated.input;


import xs.hadoop.Json;
import xs.hadoop.iterated.Collector;
import xs.hadoop.iterated.IteratedUtil;


/**
 * 封装mapper的业务,最后被DelegatingMapperHack注入到mapper
 */
public abstract class Transformer<K0,V0,K1> {
	
	/*------------------------------------------------------------*/
	
	protected Json jsonParam;
	private String scheduleName;
	private String driverIP;
	
	public void setup(Json jsonParam) throws Exception{
		this.jsonParam    = jsonParam;
		this.scheduleName = jsonParam.find("scheduleName").toString();
		this.driverIP     = jsonParam.find("driverIP").toString();
	}
	
	/*------------------------------从dirver传过来的参数----------------------------*/
	
	protected Json sourceParams;
	
	/**
	 * 得到source参数 
	 */
	public String getSourceParam(String name){
		if(sourceParams!=null) {
			Json param = sourceParams.find(name);
			return param==null?null:param.toString();
		}
		return null;
	}
	
	protected Json dirverParams;
	
	/**
	 * 得到driver main的参数 
	 */
	public String getDriverParam(int pIndex){
		if(dirverParams!=null){
			return dirverParams.find("arg"+pIndex).toString();
		}
		return null;
	}
	
	protected int sourceOrder;
	
	/**
	 * source在job中的索引,不推荐使用,应该在Transformer上分开
	 */
	public int getSourceOrder(){
		return sourceOrder;
	}
	
	
	/*------------------------------------------------------------*/
	
	public void reportDriver(Exception e){
		IteratedUtil.reportException(e, "/"+scheduleName,driverIP);
	}
	
	public void reportDriver(String message){
		IteratedUtil.reportMessage(message, "/"+scheduleName,driverIP);
	}
	
	/*------------------------------------------------------------*/
	
	public abstract Class<K1> getMapOutputKeyClass();
	
	public abstract void flatMap(K0 inputKey, V0 inputValue,Collector<K1> collector) throws Exception;
	
	/**
	 * 在mapper cleanup的时候调用 
	 * unionclose这个名字不好看,不如将close方法单独分出去
	 */
	public void union(Collector<K1> collector) throws Exception{};
	
	
}


