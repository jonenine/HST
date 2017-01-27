package xs.hadoop.iterated.netty.codec;

import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import xs.hadoop.Json;
import xs.hadoop.Json.TravelCallback;
import xs.hadoop.ResourceUtils;
import xs.hadoop.StringUtils;


/**
 * 作为单例对象而存在,实际上是一个配置类,负责解析规约的规则
 */
public class Protocols{
	
	protected static Logger logger = Logger.getLogger(Protocols.class);
	
	
	private static Protocols p;
	
	/**
	 * 得到单例对象 
	 */
	public static Protocols getProtocols(){
		if(p == null){
			p = new Protocols();
		}
		
		return p;
	}
	
	private Protocols(){}
	
	/**
	 * <协议名称,frame定义>
	 */
	Map<String, List<ItemTempltate>> frameConfigs = new HashMap<String, List<ItemTempltate>>();
	Map<String,Map<String, FunctionTemplate>> functions = new HashMap<String, Map<String,FunctionTemplate>>();
	
	
	/**
	 * 离开spring后,需要手动调用一下 
	 */
	public void setFrameConfigs(Map<String, String> frameDefinitionPaths) {
		for(String pName:frameDefinitionPaths.keySet()){
			List<ItemTempltate>  itemTempltates= retriveFrameConfig(pName,frameDefinitionPaths.get(pName));
			frameConfigs.put(pName, itemTempltates);
		}
	}
	
	public void setFunctions(Map<String, Map<String, FunctionTemplate>> functions) {
		this.functions = functions;
	}
	
	/**
	 * 手动添加一个function
	 */
	public Protocols addFunction(String pName,String fName,FunctionTemplate function) {
		Map<String, FunctionTemplate> fs = functions.get(pName);
		if(fs==null){
			fs = new HashMap<String, FunctionTemplate>();
			functions.put(pName,fs);
		}
		fs.put(fName, function);
		
		return this;
	}
	
	public FunctionTemplate getFunction(String pName,String fName){
		Map<String, FunctionTemplate> fs = functions.get(pName);
		return fs.get(fName);
	}



	/**
	 * @param configPathInBundle 别忘了以"/"开头
	 * @return
	 */
	private List<ItemTempltate> retriveFrameConfig(final String pName,String configPathInBundle){
		InputStream pio = ResourceUtils.getResourceInBundle(Protocols.class,configPathInBundle);
		try {
			//js文件要使用utf-8编码
			String configJs = ResourceUtils.getFileContent(pio, "UTF-8").trim();
			String configStr = configJs.substring(configJs.indexOf("=")+1);
			//
			final List<ItemTempltate> re = new LinkedList<ItemTempltate>();
			if(configStr!=null && (configStr=configStr.trim()).length()>0){
				Json.desierialize(configStr).travelLimitLayer(new TravelCallback(){
					public void process(String id, Json jso, Json parent, int layer) {
						//在item的级别遍历
						if(layer==0){
							ItemTempltate itemConfig = null;
							switch(jso.getType()){
								/*----------------------------------------------*/
								case string:{
									final byte value = (Byte)parseElRoughly(jso.toString());
									itemConfig = new ItemTempltate(id){
										public int length(Parser parser){
											return 1;
										}
										public Object out(Parser parser){
											return new byte[]{value};
										}
										public boolean validate(Parser parser){
											byte[] itemArray = parser.getCurrentItemArray();
											return value==itemArray[0];
										}
									};
									break;
								}
								/*----------------------------------------------*/
								case array:{
									List<Json> valueList = jso.asList();
									final byte[] values = new byte[valueList.size()];
									for(int i=0,l=valueList.size();i<l;i++){
										values[i] = (Byte)parseElRoughly(valueList.get(i).toString());
									}
									itemConfig = new ItemTempltate(id){
										public int length(Parser parser){
											return values.length;
										}
										public Object out(Parser parser){
											return values;
										}
										public boolean validate(Parser parser){
											byte[] itemArray = parser.getCurrentItemArray();
											for(int i=0,l=values.length;i<l;i++){
												if(itemArray[i]!=values[i]) return false;
											}
											return true;
										}
									};
									break;
								}
								/*----------------------------------------------*/
								case object:{
									final Json length  = jso.find("length");
									final Json out     = jso.find("out");
									final Json validate= jso.find("validate");
									
									itemConfig = new ItemTempltate(id){
										public int length(Parser parser){
											
											String lengthStr = length==null?null:length.toString().trim();
											//必须定义length
											Object elValue = parseElRoughly(lengthStr);
											if(elValue instanceof String){
												FunctionTemplate func = Protocols.this.getFunction(pName,(String)elValue);
												if(func==null) throw new RuntimeException("unknow function :"+(String)elValue);
												return  func.length(parser);
											}else{
												//转为int型的时候要注意
												return  ((Byte)elValue) & 0Xff;
											}
										}
										public Object out(Parser parser){
											String outStr = out==null?null:out.toString().trim();
											//如果定义了out
											if(outStr!=null && outStr.trim().length()>0){
												String elValue = (String)parseElRoughly(outStr);
												FunctionTemplate func = Protocols.this.getFunction(pName,elValue);
												if(func==null) throw new RuntimeException("unknow function :"+elValue);
												return  func.out(parser);
											}else{
												//如果没有定义,就返回这个项对应的byte[]
												return  parser.getCurrentItemArray();
											}
											
										}
										public boolean validate(Parser parser){
											String validateStr = validate==null?null:validate.toString().trim();
											//如果定义了validate
											if(validateStr!=null && validateStr.trim().length()>0){
												//el必须是字符类型
												String elValue = (String) parseElRoughly(validateStr);
												FunctionTemplate func = Protocols.this.getFunction(pName,elValue);
												if(func==null) throw new RuntimeException("unknow function :"+elValue);
												return  func.validate(parser);
											}else{
												return true;
											}
											
										}
									};
									break;
								}
							}//~case
							re.add(itemConfig);
						}
					}
				}, 0);
			}
			logger.info("sunncess load protocol :"+pName+" with file "+configPathInBundle);
			return re;
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally{
			if(pio!=null)
				try {
					pio.close();
				} catch (Exception e){}
		}
	}

	
	/**
	 * 粗略的解析el表达式
	 * 一共有三种类型
	 * 0X68    16进制整数
	 * 2       整数
	 * int()   方法
	 * @return
	 */
	private static Object parseElRoughly(String valueStr)throws RuntimeException{
		valueStr = valueStr.replaceAll("\\s+", "");
		if(StringUtils.getRegXp("\\d+", valueStr).isMatch()){
			return new Byte((byte)(Integer.parseInt(valueStr) & 0Xff));
		}else if(valueStr.startsWith("0X")){
			return new Byte((byte)(Integer.parseInt(valueStr.replace("0X", ""),16) & 0xff));
		}else if(valueStr.endsWith("()")){
			return valueStr.substring(0, valueStr.length()-2);
		}
		throw new RuntimeException("无法解析el:"+valueStr);
	}

	/**
	 * @param protocolName 协议名称
	 * @param parser       实现了回调业务的
	 * @return
	 */
	public Parser createParser(String protocolName,final ParseCallback callBack){
		List<ItemTempltate> frameConfig = frameConfigs.get(protocolName);
		return new Parser(frameConfig){
			public void onFrameClosed(Map<String, Object> itemsSoFar) {
				callBack.onFrameClosed(itemsSoFar);
			}

			public void onHasSomethingWrong(byte[] errorByte, String string) {
				callBack.onHasSomethingWrong(errorByte, string);
			}
		};
	}

	/**
	 * 应该定义在transport层
	 */
	public static void setupSample(){
		//setFrameConfigs
		Protocols protocols = getProtocols();
		//测试一下
		protocols.addFunction("default","int",new FunctionTemplate(){
			//可以通过返回集合来返回多值
			public Object out(Parser parser) {
				byte[] bs = parser.getCurrentItemArray();
				//假设高位在前,低位在后
				int re=0;
				for(byte b:bs) re=((int)b)&0xff+re*256;
				return re;
			}
		});
		protocols.addFunction("default","fLength",new FunctionTemplate(){
			public int length(Parser parser) {
				return (Integer)parser.getItemsSoFar().get("fLength");
			}
		});
		protocols.addFunction("default","string",new FunctionTemplate(){
			public Object out(Parser parser) {
				byte[] bytes = parser.getCurrentItemArray();
				//
				StringBuffer re = new StringBuffer();
				for(int i=0,l=bytes.length;i<l;i++){
					if(((int)bytes[i])!=0){//注意可能是c的程序将字符串最后的"\0"发了过来
						re.append((char)(0xff & (bytes[i])));
					}
				}
				return re.toString();
			}
		});
		protocols.addFunction("default","check_sum",new FunctionTemplate(){
			public boolean validate(Parser parser) {
				return true;
			}
		});
	}

	
}






