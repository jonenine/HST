package xs.hadoop;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import xs.hadoop.Json.TravelCallback;


/**
 * json和java基本类型之间的转化 
 */
public class Convertor {
	
	private static Class[] simpleTypes=new Class[]{String.class,Long.class,long.class,
			Integer.class,int.class,Double.class,double.class,Float.class,float.class,Boolean.class,boolean.class,
			java.util.Date.class,java.sql.Date.class,java.sql.Time.class,java.sql.Timestamp.class,Json.class};
	
	public static boolean isSimpleType(Class targetClass){
		for(Class simpleType:simpleTypes){
			if(targetClass.equals(simpleType)) return true;
		}
		return false;
	}
	
	public static boolean isArrayType(Class targetClass){
		if(targetClass.isArray()) return true;
		return false;
	}
	
	public static boolean isCollection(Class targetClass){
		if(Collection.class.isAssignableFrom(targetClass)) return true;
		return false;
	}
	
	public static boolean isMap(Class targetClass){
		if(Map.class.isAssignableFrom(targetClass)) return true;
		return false;
	}
	
	public static boolean isBeanType(Class targetClass){
		if(isSimpleType(targetClass)) return false;
		if(targetClass.getName().startsWith("java.")) return false;
		if(isArrayType(targetClass) || isCollection(targetClass)) return false;
		return true;
	}
	
	/**
	 * 注意number型有大小类型之分,如果是小类型,当str值为null的时候会赋值为0
	 */
	public static Object desierializeSimpleType(Class targetClass,String value){
		if(value==null) return null;
		try {
			if(String.class.equals(targetClass)){
				if(value!=null && value.length()==0) value=null;
				return value;
			}if(Long.class.equals(targetClass) || long.class.equals(targetClass)){
				return Long.parseLong(value);
			}else if(Integer.class.equals(targetClass) || int.class.equals(targetClass)){
				return Integer.parseInt(value);
			}else if(Double.class.equals(targetClass) || double.class.equals(targetClass)){
				return Double.parseDouble(value);
			}else if(Float.class.equals(targetClass) || float.class.equals(targetClass)){
				return Float.parseFloat(value);
			}else if(Boolean.class.equals(targetClass) || boolean.class.equals(targetClass)){//对boolean型的转换比较特殊
				value=value.trim().toLowerCase();
				if(value.length()==0 || value.equals("false")) return false;
				return true;
			}else if(java.util.Date.class.equals(targetClass)){//以下四个是不是能合并啊
				return StringUtils.adaptDate(value);
			}else if(java.sql.Date.class.equals(targetClass)){
				java.util.Date date=(Date) desierializeSimpleType(java.util.Date.class,value);
				return new java.sql.Date(date.getTime());
			}else if(java.sql.Time.class.equals(targetClass)){
				java.util.Date date=(Date) desierializeSimpleType(java.util.Date.class,value);
				return new java.sql.Time(date.getTime());
			}else if(java.sql.Timestamp.class.equals(targetClass)){
				java.util.Date date=(Date) desierializeSimpleType(java.util.Date.class,value);
				return new java.sql.Timestamp(date.getTime());
			}else if(Json.class.equals(targetClass)){
				return Json.desierialize(value);
			}
		} catch (Exception e) {}
		return null;
	}
	
	private static final SimpleDateFormat dateFm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static String sierializeSimpleType(Object value){
		if(value==null){
			return "null";
		}else{
			if(java.util.Date.class.isAssignableFrom(value.getClass())){
				return dateFm.format((java.util.Date)value);
			}else{
				String v = value.toString();
				v = v.replace("\n", "\\n").replace("\r", "\\r").replace("\"", "\\\"");//只是hibernate这样做,从grid出来的数据都是jdbc的
				return v;
			}
		}
	}
	
	/**
	 * 序列化为json的表示形式
	 * @param target
	 * @return
	 * @throws Exception
	 */
	public static String sierialize(Object target) throws Exception{
		if(target==null) return "null";
		
		Class targetClass=target.getClass();
		if(isArrayType(targetClass)){
			StringBuffer jsonString=new StringBuffer("[");
			for(int i=0,l=Array.getLength(target);i<l;i++){
				jsonString.append((i==0?"":",")+sierialize(Array.get(target, i)));
			}
			return jsonString.append("]").toString();
		}else if(isCollection(targetClass)){
			Collection collection=(Collection)target;
			StringBuffer jsonString=new StringBuffer("[");
			if(!collection.isEmpty()){
				int i=0;
				Iterator it = collection.iterator();
				while(it.hasNext()){
					jsonString.append((i==0?"":",")+sierialize(it.next()));
					i++;
				}
			}
			return jsonString.append("]").toString();
		}else if(isSimpleType(targetClass)){
			//ajax返回时,会以对象方式被识别
			return "\""+sierializeSimpleType(target)+"\"";
		}else if(isMap(targetClass)){
			Map map = (Map)target;
			StringBuffer jsonString=new StringBuffer("{");
			for(Object key:map.keySet()){
				Object value = map.get(key);
				jsonString.append((jsonString.length()>1?",":"")+sierialize(key.toString())+":\""+sierialize(value)+"\"");
			}
			return jsonString.append("}").toString();
		}else if(isBeanType(targetClass)){
			StringBuffer jsonString=new StringBuffer("{");
			for(Method method:targetClass.getMethods() ){
				String methodName=method.getName();
				Class<?>[] paramTypes = method.getParameterTypes();
				if(methodName.startsWith("set") && paramTypes.length==1){
					//得到表单的名称
					String inputName;
					inputName=methodName.substring(3);
					inputName=inputName.substring(0,1).toLowerCase()+inputName.substring(1);
					//得到getter,确定返回类型
					Method getter = null;
					try {
						getter = targetClass.getMethod("get"+methodName.substring(3), new Class[0]);
					} catch (RuntimeException e) {}
					if(getter == null) continue;
					//临时增加,lob类型不进行序列化,意味着lob类型在数据库设计时最好单独做一张表
					Class retrunType = getter.getReturnType();
					if(java.sql.Blob.class.isAssignableFrom(retrunType) || java.sql.Clob.class.isAssignableFrom(retrunType)) continue;
					//
					Object returnValue=getter.invoke(target, new Object[0]);
					jsonString.append((jsonString.length()>1?",":"")+"\""+inputName+"\":"+sierialize(returnValue));
				}
			}
			//添加类型属性
			jsonString.append((jsonString.length()>1?",":"")+"\"class\":"+sierialize(sierialize(target.getClass().getName())));
			return jsonString.append("}").toString();
		}
		return "null";
	}
	
	public static Object desierialize(Json json,Class targetClass) throws Exception{
		/*-----------反序列化null,类似于{editValue:""}对象也返回null------*/
		if(json.value()==null || json.value().length()==0) return null;
		
		/*-----------反序列化基本类型---*/
		Object simpleObject = desierializeSimpleType(targetClass, json.value());
		if(simpleObject!=null) return simpleObject;
		
		/*-----------对象类型----------*/
		//如果json中定义了类型
		String className = json.find("class").value();
		if(className!=null){
			//有时类中属性的定义是父类,而实际的赋值是子类
			Class subClass = Thread.currentThread().getContextClassLoader().loadClass(className);
			if(targetClass==null || targetClass.isAssignableFrom(subClass)) targetClass = subClass;
		}
		if(targetClass==null) return null;
		//默认构造器
		Object object = targetClass.getConstructor(new Class[0]).newInstance(new Object[0]);
		Method[] methods = targetClass.getMethods();
		if(methods!=null && methods.length>0){
			for(Method method:methods){
				String methodName=method.getName();
				Class<?>[] paramTypes = method.getParameterTypes();
				//得到所有的setter
				if(methodName.startsWith("set") && paramTypes.length==1){
					//得到属性名称
					String _fieldName=methodName.substring(3);
					String fieldName=_fieldName.substring(0,1).toLowerCase()+_fieldName.substring(1);
					
					//转换值---这里有一个小漏洞,先按照首字母小写的方式取,再按照不小写的形式取(不规范的bean prop name)
					Json _fieldJson = json.find(fieldName);
					if(_fieldJson.value() == null) _fieldJson = json.find(_fieldName);
					
					if(_fieldJson.value() == null) continue;
					Class  fieldClass = paramTypes[0];
					Object fieldValue = null;
					//只支持数组,简单类型和bean和map
					if(isArrayType(fieldClass)){
						Class componentClass = fieldClass.getComponentType();
						List<Json> list = _fieldJson.asList();
						int length = list.size();
						fieldValue = Array.newInstance(componentClass, length);
						for(int i=0;i<length;i++){
							Array.set(fieldValue, i, desierialize(list.get(i),componentClass));
						}
					}else if(isSimpleType(fieldClass) || isBeanType(fieldClass)){
						fieldValue = desierialize(_fieldJson,fieldClass);
					}
					//以下两个类型的说明必须在json中
					else if(isMap(fieldClass)){
						final Map<String,Object> map = (Map<String,Object>) fieldClass.getConstructor(new Class[0]).newInstance(new Object[0]);
						json.travelLimitLayer(new TravelCallback(){
							public void process(String id, Json _elementJson, Json parent, int layer) {
								if(layer==0){
									Object element = null;
									try {
										element = desierialize(_elementJson,null);
									} catch (Exception e) {}
									if(element!=null) map.put(id, element);
								}
							}
						},0);
						fieldValue = map;
					}else if(isCollection(fieldClass)){
						Collection col = (Collection)fieldClass.getConstructor(new Class[0]).newInstance(new Object[0]);
						for(Json _elementJson:_fieldJson.asList()){
							Object element = desierialize(_elementJson,null);
							if(element!=null) col.add(element);
						}
						fieldValue = col;
					}
					//设置这个setter
					if(fieldValue!=null) method.invoke(object, fieldValue);
				}
			}
		}
		return object;
	}
	
	
	public static void main(String[] args){
		Json jso = Json.desierialize("{\"editCode\":\"TEAM_ID\",\"editName\":\"抄表组\",\"editValue\":\"\"}");
		String className = jso.find("class").value();
		System.out.println(className);
	}
	
}





















