package xs.hadoop.iterated;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;

import xs.hadoop.Json;
import xs.hadoop.StringUtils;
import xs.hadoop.iterated.netty.MessageClient;
import xs.hadoop.iterated.netty.MessageServer;

import com.google.common.base.Throwables;



/**
 * 工具类，提供一些基础的功能
 */
public class IteratedUtil {
	
	/*---------------------------------资源类的---------------------------------*/
	
	static String baseHdfsUri;
	
	public static String getBaseHdfsUri() {
		return baseHdfsUri;
	}


	/**
	 * hadoop的输出及文件所在的hdfs
	 * 设置基础路径,在集群模式下不用,放到哪个集群下面,就在哪个集群的hdfs下面
	 */
	public static void setBaseHdfsUri(String baseHdfsUri) {
		IteratedUtil.baseHdfsUri = baseHdfsUri;
	}


	public static String qualifiedPath(String path) throws Exception{
		//本地文件路径的时候,不是通过FileInputFormat进行的qualified
		if(path.startsWith("file:///")) return path.replace("file:///", "file:/");
		
		
		return path;
		/*
		Configuration conf = new Configuration();
		Job jobCopy = Job.getInstance(new Configuration());
		FileInputFormat.setInputPaths(jobCopy, new Path[]{new Path(path)});
		return conf.get(FileInputFormat.INPUT_DIR);
		*/
	}
	
	
	/*
	* 看了下源码,DelegatingInputFormat 110行 -->调用--> FileInputFormat 453行 conf.set(INPUT_DIR, str.toString());
	* 可以从conf中得到这个path,不行就为每个mapper建立一个path(hdfs),里面有固定名称的文件,从文件中读配置好了
	* 仔细看DelegatingInputFormat 110-115行,实际是循环使用jobCopy的
	*/
	
	@Deprecated
	public static String getParamterString(Configuration conf,String path) throws Exception{
		//注意这个是本地路经---最后验证一下是否可以得到path即可????
		String paramaterFilePath = IteratedUtil.rightSlash(IteratedUtil.leftSlash(IteratedUtil.getParentPath(path),true),true)+"paramters.conf";
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(paramaterFilePath))) path = paramaterFilePath;
		fs.close();
		
		//得到配置信息
		String params = IteratedUtil.readFileContent(path, baseHdfsUri);
		
		return params;
	}
	
	
	/**
	 * 将配置项写入一个临时文件,并返回路经 
	 */
	public static String createParamterFile(Job job,Json parameters) throws Exception{

		//创建临时文件
		String category = job.getJobName()+"/inputFormatParams";
		Object[] tempFile = IteratedUtil.createTempFile(category, "paramters.conf", baseHdfsUri);
		String file = (String) tempFile[0];
		FSDataOutputStream out = (FSDataOutputStream) tempFile[1];
		
		//将配置写入文件
		IteratedUtil.writeFileContent(out, parameters.toString());
		
		return IteratedUtil.rightSlash(baseHdfsUri,false)+file;
	}
	
	
	/**
	 * 在已经有数据的path的上一级创建 paramters.conf文件,所以已经存在的数据需要  "a/b/datas/"之类的格式,配置文件为"a/b/paramters.conf"
	 */
	public static String createParamterFile(String path,Json parameters) throws Exception{

		path = IteratedUtil.getParentPath(path);
		
		/**
		 * 这里可能会出现没权限的错误
		 */
		Object[] tempFile = IteratedUtil.createFile(path, "paramters.conf", baseHdfsUri);
		String file = (String) tempFile[0];
		FSDataOutputStream out = (FSDataOutputStream) tempFile[1];
		
		//将配置写入文件
		IteratedUtil.writeFileContent(out, parameters.toString());
		
		return file;
	}
	
	static Random r = new Random();
	
	/**
	 * 得到随机路经,前后都没有"/" 
	 * @throws InedException 
	 */
	public synchronized static String getRandomSubPath() throws InterruptedException{
		String random = (r.nextDouble()+"_"+System.currentTimeMillis()).substring(2);
		Thread.sleep(2);
		return random;
	}
	
	public static String leftSlash(String str,boolean hasSlash){
		if(str.startsWith("/")) str = str.substring(1);
		return hasSlash?("/"+str):str;
	}
	
	public static String rightSlash(String str,boolean hasSlash){
		if(str.endsWith("/")) str = str.substring(0,str.length()-1);
		return hasSlash?(str+"/"):str;
	}
	
	public static String getParentPath(String path){
		int lastIndex = path.lastIndexOf("/");
		lastIndex = lastIndex==-1? path.lastIndexOf("\\"):lastIndex;
		path = path.substring(0,lastIndex);
		return path;
	}
	
	
	public static String[] listFileInDir(String dirPath,String hdfsUri)throws Exception{
		FileSystem fs;
		if(hdfsUri!=null){
			fs = FileSystem.get(URI.create(hdfsUri), new Configuration());
		}else{
			fs = FileSystem.get(new Configuration());
		}
		
		Path path = new Path(dirPath);
		if(fs.exists(path)){
			if(fs.isDirectory(path)){
				//不对这个目录进行递归
				RemoteIterator<LocatedFileStatus> paths = fs.listFiles(path, false);
				List<String> pathList = new LinkedList<String>();
				while(paths.hasNext()){
					LocatedFileStatus next = paths.next();
					if(next.isFile()){
						String fileName = next.getPath().toString();
						if(!fileName.endsWith("_SUCCESS")) pathList.add(fileName);
					}
				}
				
				return pathList.toArray(new String[pathList.size()]);
			}
			//
			else{
				return new String[]{dirPath};
			}
		}
		
		return null;
	}
	
	/**
	 * 找到一个还无人使用的文件夹,注意不要创建目录,hadoop自己会创建
	 */
	public static String findUnusedTempDir(String category,String hdfsUri) throws Exception{
		FileSystem fs;
		if(hdfsUri!=null){
			fs = FileSystem.get(URI.create(hdfsUri), new Configuration());
		}else{
			fs = FileSystem.get(new Configuration());
		}
		
		
		Path path;
		do{
			String dir = "/tmp"+rightSlash(leftSlash(category,true),true)+getRandomSubPath();
			path = new Path(dir);
		}while(fs.exists(path));
		
		return path.toString();
	}
	
	
	/**
	 * 创建临时文件夹 
	 * @throws Exception
	 */
	public static String createTempDir(String category,String hdfsUri) throws Exception{
		FileSystem fs;
		if(hdfsUri!=null){
			fs = FileSystem.get(URI.create(hdfsUri), new Configuration());
		}else{
			fs = FileSystem.get(new Configuration());
		}
		
		
		Path path;
		do{
			String dir = "/tmp"+rightSlash(leftSlash(category,true),true)+getRandomSubPath();
			path = new Path(dir);
		}while(fs.exists(path));
		
		//创建目录
		fs.mkdirs(path);
		return path.toString();
	}
	/**
	 * 创建临时文件  创建/tmp/category/random/filename
	 * @param category 
	 * @param fileName
	 * @param hdfsUri
	 * @throws Exception
	 */
	public static Object[] createTempFile(String category,String fileName,String hdfsUri) throws Exception{
		String path = createTempDir(category,hdfsUri);
		
		String file = path+"/"+fileName;
		
		FileSystem fs;
		if(hdfsUri!=null){
			fs = FileSystem.get(URI.create(hdfsUri), new Configuration());
		}else{
			fs = FileSystem.get(new Configuration());
		}
		
		//创建文件
		FSDataOutputStream out = fs.create(new Path(file));
		
		return new Object[]{file,out};
	}
	
	/**
	 * 在指定目录创建文件
	 * @param prefixPath  指定目录,如果指定目录不存在,就创建
	 * @param fileName
	 * @param hdfsUri
	 * @return
	 * @throws Exception
	 */
    public static Object[] createFile(String prefixPath,String fileName,String hdfsUri) throws Exception{
		
    	FileSystem fs;
		if(hdfsUri!=null){
			fs = FileSystem.get(URI.create(hdfsUri), new Configuration());
		}else{
			fs = FileSystem.get(new Configuration());
		}
		
		//创建目录
		String dir = rightSlash(leftSlash(prefixPath,true),true);
		Path path = new Path(dir);
		if(!fs.exists(path)) fs.mkdirs(path);
		
		String file = path.toString()+"/"+fileName;
		//创建文件
		FSDataOutputStream out = fs.create(new Path(file));
		
		return new Object[]{file,out};
	}
	
    
	public static String readFileContent(String filePath,String hdfsUri) throws IOException{
		FSDataInputStream is = null;
		try {
			FileSystem fs;
			if(hdfsUri!=null){
				fs = FileSystem.get(URI.create(hdfsUri), new Configuration());
			}else{
				fs = FileSystem.get(new Configuration());
			}
			
			is  = fs.open(new Path(filePath));
			InputStreamReader isr = new InputStreamReader(is, "UTF-8");
			BufferedReader br = new BufferedReader(isr, 1024);
			
			String line = null;
			StringBuilder sb = new StringBuilder();
			while((line=br.readLine())!=null){
				sb.append(line+" ");
			}
			
			return sb.toString();
		}catch(Exception e){
			throw new IOException(e);
		}finally{
			if(is!=null)
				try {
					is.close();
				} catch (Exception e) {}
		}
	}
	
	public static void writeFileContent(OutputStream out,String content) throws IOException{
		try {
			OutputStreamWriter outr = new OutputStreamWriter(out, "UTF-8");
			BufferedWriter bw = new BufferedWriter(outr, 1024);
			
			bw.write(content);
			bw.flush();
			bw.close();
		} finally{
			try {
				out.close();
			} catch (Exception e) {}
		}
	}
	
	/*--------------------------------------类型转换----------------------------------------*/
	
	/**
	 * 将java类型和数组类型和map类型,转换为hadoop writable类型
	 * 不支持list,set等其他类型
	 */
	public static Writable toHadoopWritable(Object object){
		if(object == null){
			return NullWritable.get();
		}else if(object instanceof Integer){
			return new IntWritable((Integer)object);
		}else if(object instanceof Long){
			return new LongWritable((Long)object);
		}else if(object instanceof Float){
			return new FloatWritable((Float)object);
		}else if(object instanceof Double){
			return new DoubleWritable((Double)object);
		}else if(object instanceof String){
			return new Text(object.toString());
		}else if(object.getClass().isArray()){
			int length = Array.getLength(object);
			if(length>0){
				List<Writable> list = new ArrayList<Writable>(length);
				for(int i=0;i<length;i++){
					list.add(toHadoopWritable(Array.get(object, 1)));
				}
				
				ArrayWritable aw = new ArrayWritable(list.get(0).getClass());
				aw.set(list.toArray(new Writable[length]));
				
				return aw;
			}
			//空数组等同于null
			else{
				return NullWritable.get();
			}
		}else if(object instanceof Map){
			Map map = (Map)object;
			MapWritable mw = new MapWritable();
			boolean isEmpty = true;
			for(Object key:map.keySet()){
				mw.put(toHadoopWritable(key), toHadoopWritable(map.get(key)));
				if(isEmpty) isEmpty = false;
			}
			
			if(isEmpty){
				return NullWritable.get();
			}else{
				return mw;
			}
		}
		
		throw new RuntimeException("can't transfer to hadoop type from java type "+object.getClass());
	}
	
	/**
	 * 将hadoop数据类型,自动转换为java数据类型,这里要提前对类型之间的转换关系做一个约定 
	 */
	public static Object toJavaObject(Object hadoopObject){
		if(hadoopObject.equals(NullWritable.get())){
			return null;
		}else if(hadoopObject instanceof IntWritable){
			return ((IntWritable)hadoopObject).get();
		}else if(hadoopObject instanceof LongWritable){
			return ((LongWritable)hadoopObject).get();
		}else if(hadoopObject instanceof FloatWritable){
			return ((FloatWritable)hadoopObject).get();
		}else if(hadoopObject instanceof DoubleWritable){
			return ((DoubleWritable)hadoopObject).get();
		}else if(hadoopObject instanceof Text){
			return ((Text)hadoopObject).toString();
		}else if(hadoopObject instanceof ArrayWritable){
			Writable[] wa = ((ArrayWritable)hadoopObject).get();
			int length = wa.length;
			
			if(wa==null || wa.length==0) return null;
			
			Object ele0 = toJavaObject(wa[0]);
			Object array = Array.newInstance(ele0.getClass(), length);
			Array.set(array, 0, ele0);
			
			for(int i=1;i<wa.length;i++){
				Array.set(array, i, toJavaObject(wa[i]));
			}
			
			return array;
		}else if(hadoopObject instanceof MapWritable){
			Set<Entry<Writable, Writable>> set = ((MapWritable)hadoopObject).entrySet();
			if(set==null || set.size()==0) return null;
			
			Map map = new HashMap();
			for(Entry<Writable, Writable> entry:set){
				map.put(toJavaObject(entry.getKey()), toJavaObject(entry.getValue()));
			}
			
			return map;
		}
		
		throw new RuntimeException("can't transfer to java type from hadoop type "+hadoopObject.getClass());
	}
	
	
	public static Class<? extends WritableComparable> toHadoopWritableComparableClass(Class clazz){
		if(clazz.equals(Integer.class)){
			return IntWritable.class;
		}else if(clazz.equals(Long.class)){
			return LongWritable.class;
		}else if(clazz.equals(Float.class)){
			return FloatWritable.class;
		}else if(clazz.equals(Double.class)){
			return DoubleWritable.class;
		}else if(clazz.equals(String.class)){
			return Text.class;
		}
		
		throw new RuntimeException("can't transfer to hadoop WritableComparable type from java type "+clazz);
	}
	
	public static WritableComparable toHadoopWritableComparable(Object object){
		if(object instanceof Integer){
			return new IntWritable((Integer)object);
		}else if(object instanceof Long){
			return new LongWritable((Long)object);
		}else if(object instanceof Float){
			return new FloatWritable((Float)object);
		}else if(object instanceof Double){
			return new DoubleWritable((Double)object);
		}else if(object instanceof String){
			return new Text(object.toString());
		}
		
		throw new RuntimeException("can't transfer to hadoop WritableComparable from java type "+object.getClass());
	}
	
	
	/**Writable
	 * merge重复的时候 ,要如何处理这个值
	 */
	public static interface MergeRepeat{
		/**
		 * @param oldMaps 这个重复的值曾经出现过的map 
		 * @param newMap  当前的新map
		 */
		Object on(String fieldName,List<Map<String,Object>> oldMaps,Map<String,Object> newMap);
	}
	
	/**Writable
	 * 将不同的map合并为一个,常用于列转行 
	 * @param onRepeat 如果为空,则简单的使用后面的覆盖前面的
	 */
	public static Map<String,Object> mergeMap(List<Map<String,Object>> values,MergeRepeat onRepeat){
		Map<String,Object> re = new HashMap();
		
		Map<String , List<Map<String,Object>>> cache = null;
		if(onRepeat!=null) cache = new HashMap();
		
		for(Map<String,Object> map:values){
			for(String key:map.keySet()){
				if(onRepeat == null){
					re.put(key, map.get(key));
				}else{
					if(re.containsKey(key)){
						List<Map<String,Object>> oldMaps = cache.get(key);
						
						Object result = onRepeat.on(key, oldMaps, map);
						if(result!=null) re.put(key, result);
						
						//添加oldMaps
						oldMaps.add(map);
					}else{
						re.put(key, map.get(key));
						
						//添加oldMaps
						List<Map<String,Object>> oldMaps = new LinkedList();
						cache.put(key, oldMaps);
						
						oldMaps.add(map);
					}
				}
			}
		}
		
		return re;
	}
	
	/**
	 * 将reducer参数转换为java类型,并按照_order分组 ,既在返回list的索引等于_order
	 */
	public static List<List<Map<String,Object>>> group(Iterable<MapWritable> reducerValues){
		
		List<Integer> orders = new LinkedList();
		List<List<Map<String,Object>>> temp = new LinkedList();
		
		int maxOrder = -1;
		for(MapWritable mw:reducerValues){
			Map<String, Object> map = (Map<String, Object>) toJavaObject(mw);
			int order = Integer.parseInt(map.get("_order").toString());
			if(order > maxOrder) maxOrder = order;
			
			List<Map<String,Object>> maps;
			int index = orders.indexOf(order);
			if(index>-1){
				maps = temp.get(index);
			}else{
				maps =  new LinkedList();
				//
				orders.add(order);
				temp.add(maps);
			}
			
			maps.add(map);
		}
		
		List<List<Map<String,Object>>> re = new ArrayList(maxOrder+1);
		
		
		for(int i=0;i<=maxOrder;i++){
			int index = orders.indexOf(i);
			if(index>-1){
				re.add(temp.get(index));
			}else{
				re.add(null);
			}
			
		}
		
		
		return re;
	}
	
	/**
	 * 使用map中field再次分组,通常是使用ValueHandler中getValue方法的返回值
	 * 使用的场景是:无法在source阶段进行区分,比如一个文件中,既有昨天的数据,又有今天的数据,然后要求按照某个key join之后在不同日期的数据之间做计算
	 */
	public static Map<String,List<Map<String,Object>>> group(String groupFieldName,List<Map<String,Object>>... values){
		
		Map<String,List<Map<String,Object>>> re = new HashMap();
		
		for(List<Map<String,Object>> maps:values){
			for(Map<String,Object> map:maps){
				String groupName = map.get(groupFieldName).toString();
				
				List<Map<String,Object>> groupMaps = re.get(groupName);
				if(groupMaps == null){
					groupMaps = new LinkedList();
					re.put(groupName, groupMaps);
				}
				
				groupMaps.add(map);
			}
		}
		
		return re;
	}
	
	public static <T> T transfor(String str,Class<T> clazz){
		if(str == null || (str = str.trim()).length()==0) return null;
		try {
			if(clazz.equals(Integer.class)){
				Integer t = Integer.parseInt(str);
				return (T) t;
			}else if(clazz.equals(Long.class)){
				Long t = Long.parseLong(str);
				return (T) t;
			}else if(clazz.equals(Float.class)){
				Float t = Float.parseFloat(str);
				return (T) t;
			}else if(clazz.equals(Double.class)){
				Double t = Double.parseDouble(str);
				return (T) t;
			}else if(clazz.equals(String.class)){
				return (T) str;
			}
		} catch (Exception e) {}
		
		return null;
	}
	
	/**
	 * 将字符串拆分并转换，忽略异常,这对于字符串类型的key是经常要用到的，通常一个key包括了太多的内容
	 */
	public static <T> T splitAndTransfor(String str,char splitChar,int index,Class<T> clazz) {
		try {
			String[] strs = str.split("\\"+splitChar);
			if(strs.length>index){
				return transfor(strs[index],clazz);
			}
		} catch (Exception e) {}
		
		return null;
	}
	
	public static <T> T getFieldFromRow(Map<String, String> row,String fieldName,Class<T> clazz) {
		return transfor(row.get(fieldName),clazz);
	}
	
	public static <T> Map<String, T> getFieldsFromRow(Map<String, String> row,Class<T> clazz,Set<String> excludesFields) {
		
		Map<String, T> re = new HashMap<String, T>();
		for(String fieldName:row.keySet()){
			if(excludesFields!=null && excludesFields.contains(fieldName)) continue;
			
			T t = transfor(row.get(fieldName),clazz);
			//空值同样过滤出去
			if(t!=null) re.put(fieldName, t);
		}
		return re;
	}
	
	
	
	/**
	 *  mulity用于减少精度
	 */
	public static  Map<String, Long> getLongFieldsFromRow(Map<String, String> row,int mulity) {
		Map<String, Long> re = new HashMap<String, Long>();
		for(String fieldName:row.keySet()){
			Float t = transfor(row.get(fieldName),Float.class);
			//空值同样过滤出去
			if(t!=null) re.put(fieldName, new Float(t*mulity).longValue());
		}
		return re;
	}
	
	public static boolean isNotEmpty(Object object){
		if(object==null) return false;
		if(object instanceof String && object.toString().trim().length()==0) return false;
		
		return true;
	}
	
	public static boolean isAllNotEmpty(Object... objects){
		if(objects==null) return false;
		for(Object object:objects) if(!isNotEmpty(object)) return false;
		return true;
	}
	/*-------------------------计算--------------------------*/
	
	public static interface JoinCallback<T>{
		public void select(String key,Object leftValue,Object rigthValue);
		public T result();
	}
	
	/**
	 * 两边都有这个键 
	 */
	public static <T> T innerJoin(Map<String, ?> left,Map<String, ?> right,JoinCallback<T> callback){
		if(left==null || right==null) return null;
		
		for(String fieldName:left.keySet()){
			Object leftValue  = left.get(fieldName);
			Object rightValue = right.get(fieldName);
			if(leftValue!=null && rightValue!=null){
				callback.select(fieldName, leftValue, rightValue);
			}
		}
		return callback.result();
	}
	
	
	/**
	 * 任意一方有这个键 
	 */
	public static <T> T outerJoin(Map<String, ?> left,Map<String, ?> right,JoinCallback<T> callback){
		if(left!=null ){
			for(String fieldName:left.keySet()){
				Object leftValue  = left.get(fieldName);
				Object rightValue = right==null?null:right.remove(fieldName);
				callback.select(fieldName, leftValue, rightValue);
			}
		}
		
		if(right!=null){
			for(String fieldName:right.keySet()){
				Object rightValue = right.get(fieldName);
				callback.select(fieldName, null, rightValue);
			}
		}
		
		return callback.result();
	}
	
	public static <T> T nvl(T num,T alternative){
		if(num==null) return alternative;
		return num;
	}
	
	
	/*------------------------通信部分-------------------------*/
    public static MessageServer messageServer;
	
	public static MessageServer getMessageManager() throws Exception{
		if(messageServer==null){
			messageServer = new MessageServer();
			//绑定端口---这个应该改成随机端口？？？？
			messageServer.bind(23721);
		}
		
		return messageServer;
	}
	
	/**
	 * 用于客户端向服务器端通信 
	 */
	public static void reportDriver(Json json,String scheduleURL,String driverIP){
		json.addOrUpadte(".", "url", new Json(scheduleURL));
		try {
			MessageClient.sendMessageAsync(driverIP, 23721, json.toString().getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {}
	}
	
	/**
	 * 报告一个异常
	 */
	public static void reportException(Exception exception,String scheduleURL,String driverIP){
		String error = Throwables.getStackTraceAsString(exception);
		Json json = Json.createJson();
		json.addOrUpadte(".", "error", new Json(error));
		reportDriver(json,scheduleURL,driverIP);
	}
	
	public static void reportMessage(String message,String scheduleURL,String driverIP){
		Json json = Json.createJson();
		json.addOrUpadte(".", "message", new Json(message));
		reportDriver(json,scheduleURL,driverIP);
	}
	
	public static void destoryResource(){
		if(messageServer!=null) messageServer.close();
		
		/**
		 * 这个类中所有的fs都没有close???
		 */
		String hdfsUri = getBaseHdfsUri();
		FileSystem fs = null;
		try {
			if(hdfsUri!=null){
				fs = FileSystem.get(URI.create(hdfsUri), new Configuration());
			}else{
				fs = FileSystem.get(new Configuration());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			if(fs!=null)
				try {
					fs.close();
				} catch (IOException e) {}
		}
	}
	
	
	private static String[] loalIpHostName;
	/**
	 * 得到本机ip,这里注意一下,有时一台服务器会有多个网卡
	 * 主机名代替ip，ip可能因为安装多个网卡的原因是多个??
	 */
	public static String[] getLoalIpHostName() throws Exception{
		if(loalIpHostName == null){
			Enumeration<NetworkInterface> itfs = NetworkInterface.getNetworkInterfaces();
			while(itfs.hasMoreElements()){
				NetworkInterface itf = itfs.nextElement();
				Enumeration<InetAddress> ias = itf.getInetAddresses();
				while(ias.hasMoreElements()){
					InetAddress ia = ias.nextElement();
					String ip = ia.getHostAddress().trim();
					//判断是ipv4地址
					if(ip.contains(".") && !ip.equals("127.0.0.1")){
						String hostName = ia.getHostName().trim();
						loalIpHostName = new String[]{ip,hostName};
					}
				}
			}
		}
		
		if(loalIpHostName!=null) return loalIpHostName;
		else throw new RuntimeException("can't find local ip and hostname");
	}
	
	public static boolean isMonthLastDay(Date date){
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		int lastDay = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
		if(cal.get(Calendar.DAY_OF_MONTH)==lastDay) return true;
		return false;
	}
	
	public static boolean isYearLastDay(Date date){
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		int lastDay = cal.getActualMaximum(Calendar.DAY_OF_YEAR);
		if(cal.get(Calendar.DAY_OF_YEAR)==lastDay) return true;
		return false;
	}
	
	
	
	
}






