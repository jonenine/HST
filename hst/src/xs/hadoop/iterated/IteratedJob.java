package xs.hadoop.iterated;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import xs.hadoop.Json;
import xs.hadoop.iterated.input.DelegatingMapperHack;
import xs.hadoop.iterated.input.InjectionMapper;
import xs.hadoop.iterated.input.MulitiSource;
import xs.hadoop.iterated.input.Source;
import xs.hadoop.iterated.input.Transformer;
import xs.hadoop.iterated.input.File.ParamsTextInputFormat;
import xs.hadoop.iterated.output.InjectionReducer;
import xs.hadoop.iterated.output.Joiner;


/**
 * @param <K1> 是mapper对应的输出key的类型,注意是简单的java类型,不是hadoop类型
 * 而且这个范型也没有约束作用
 */
public class IteratedJob<K1> extends Arranged{
	
	Configuration conf;
	
	String jobFullName;
	
	public void setJobFullName(String jobName) {
		this.jobFullName = jobName;
	}
	
	
	public String getJobFullName() {
		return jobFullName;
	}
	

	public IteratedJob(Configuration conf,String name) {
		this.conf = conf;
		this.name = name;
	}
	
	public IteratedJob(String jobName) {
		this.conf = new Configuration();
		this.jobFullName = jobName;
	}
	
	
	List<Source> sources  = new LinkedList<Source>();
	List<Class<? extends Transformer>> transfomerClasses = new LinkedList<Class<? extends Transformer>>();
	
	/**
	 * 缓存input和transfomerClass，并建立联系
	 */
	public <K0,V0,V1> IteratedJob from(Source<K0,V0> source,Class<? extends Transformer<K0,V0,K1>> transfomerClass){
		sources.add(source);
		transfomerClasses.add(transfomerClass);
		return this;
	}
	
	/**
	 * 多个source共享一个order（source index）和transformer的情况
	 */
	public <K0,V0,V1> IteratedJob from(Source<K0,V0>[] sourceArray,Class<? extends Transformer<K0,V0,K1>> transfomerClass){
		sources.add(sourceArray[0]);
		transfomerClasses.add(transfomerClass);
		
		for(int i=1;i<sourceArray.length;i++){
			Source source = sourceArray[i];
			source.setOrderAsPrevious(true);
			sources.add(source);
			transfomerClasses.add(transfomerClass);
		}
		
		return this;
	}
	
	/**
	 * 寻找以前的执行结果,作为此次job的输入,最后返回IteratedJob，延续IteratedJob的方法链
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public IteratedJob fromPrevious(Arranged arranged,Class<? extends Transformer<Long,String,K1>> transfomerClass) throws Exception{
		final String jobName = arranged.getName();
		
		MulitiSource mulitiSource = new MulitiSource(ParamsTextInputFormat.class) {
			protected Json[] createJsonParams() throws Exception {
				Json previousResult = IteratedJob.this.parnet.getPreviousResult(jobName);
				//设置lastOutputfieldNamesOrder
				IteratedJob.this.jobConf.set("xs.hadoop.piece.iterated.lastOutputfieldNamesOrder", previousResult.find("lastOutputfieldNamesOrder").toString());
				//这应该是个文件夹
				String inputPath  = previousResult.find("lastOutputPath").toString();
				String[] subFiles = IteratedUtil.listFileInDir(inputPath, IteratedUtil.getBaseHdfsUri());
				
				Json[] jsons = new Json[subFiles.length];
				for(int i=0;i<subFiles.length;i++){
					Json json = Json.createJson();
					json.addOrUpadte(".", "filePath", new Json(subFiles[i]));
					jsons[i] = json;
				}
				
				return jsons;
			}
		};
		
		return from(mulitiSource,transfomerClass);
	}
	
	/**
	 * 创建别名
	 */
	public IteratedJob as(String asSourceName){
		return this;
	}
	
	
	Class<? extends Joiner> joinerClass;
	
	
	/**
	 * 设置joiner Class
	 */
	public <K2,V2> IteratedJob join(Class<? extends Joiner<K1,K2,V2>> joinerClass){
		this.joinerClass = joinerClass;
		//
		return this;
	}
	
	/**
	 * 实际上是设置保存到文件时的序列化方式,这里设置列的保存顺序
	 * 在这个job之后的job也是从这个属性将上一步的结果进行反序列化,而不用在编程的时候重新设置 
	 */
	String[] fieldNamesOrder;

	public IteratedJob format(String... fieldNamesOrder){
		this.fieldNamesOrder = fieldNamesOrder;
		return this;
	}
	
	public String[] getFieldNamesOrder() {
		return fieldNamesOrder;
	}
	
	/**
	 * 设置source处理时的线程数量,进程并行度由inputFormat负责
	 * 如果不设置会有一个默认的并发数
	 */
	public IteratedJob parallelism(int numThread){
		return this;
	}
	
	/**
	 * 实际是reduce阶段的进程并行度
	 */
	int numOfReduceProcess = 5;
	public IteratedJob parallelism(int numProcess,int numThread){
		if(numOfReduceProcess>0) numOfReduceProcess = numProcess;
		return this;
	}
	
	
	/**
	 * 通过内置任务转存为其他形式  
	 */
	public void to(){
		
	}
	
	/**
	 * to的特例，再次按照某一输出列进行排序并重新存储
	 */
	public void orderBy(){
		
	}
	
	
	/**
	 * 取得结果集的前n条回到driver,用于调试或者远程实时调用
	 */
	public Iterator top(Long n){
		return null;
	}
	
	
	
	Job hadoopJob;
	Configuration jobConf;
	
	public void run(String outputPath) throws Exception{
		hadoopJob = Job.getInstance(conf);
		
		//自定义参数要设置在jobConf中,而不是conf中
		jobConf = hadoopJob.getConfiguration();
		
		//默认优化，可以去掉
		jobConf.set("mapreduce.map.java.opts", "-Xmx1536m -Xms1536m -Xmn800m");
		jobConf.set("mapreduce.reduce.java.opts", "-Xmx3000m -Xms3000m -Xmn1400m");
		
		//启用压缩
		jobConf.set("mapreduce.map.output.compress", "true");
		jobConf.set("mapred.map.output.compression.type", "block");
		jobConf.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
		
		hadoopJob.setJobName(jobFullName);
		
		final Json driverParams = IteratedJob.this.getScheduler().getJson();
		
		//整个任务有一个配置项,放到conf中
		final Json jobParameters = Json.createJson();
		final Map<String, Integer> sourceOrders = new HashMap<String, Integer>();
		final Class[] mapOutputKeyClass = new Class[1];
		
		Function func= new Function() {
			
			boolean hasSetJar = false;
			int sourceOrder = -1;
			
			public void apply(Source source,Class transformerClass) throws Exception {
				//如果是MulitiSource类型,则递归一下
				if(source instanceof MulitiSource){
					MulitiSource mulitiSource = (MulitiSource)source;
					for(Source subSource:mulitiSource.getSources()) apply(subSource,transformerClass);
					return;
				}
				
				//设置mapper的key class
				if(mapOutputKeyClass[0] == null) {
					Transformer t0 = ReflectionUtils.newInstance(transformerClass, null);
					mapOutputKeyClass[0] = t0.getMapOutputKeyClass();
				}
				
				//set jar
				if(!hasSetJar){
					hadoopJob.setJarByClass(transformerClass);
					hasSetJar = true;
				}
				
				Json parameters = new Json(new Object());
				//将属性名定义为常量???
				parameters.addOrUpadte(".", "transformerClassName",   new Json(transformerClass.getName()));
				
				//添加原始配置
				source.getJsonStr();
				parameters.addOrUpadte(".", "sourceInitParams", source.getJson());
				
				//添加driver的参数到实际的计算单位
				if(driverParams!=null) parameters.addOrUpadte(".", "driverParams", driverParams);
				
				Class inputFormatClass = source.getInputFormatClass();
				
				String path;
				if(ParamsTextInputFormat.class.isAssignableFrom(inputFormatClass)){
					//得到配置的路经
					path = source.getJson().find("filePath").toString();
				}else{
					//在hdfs上创建一个路经,并写入配置项(写入配置项这个功能已没用)
					path = IteratedUtil.createParamterFile(hadoopJob, parameters);
				}
				
				//得到规范的书写方式
				path = IteratedUtil.qualifiedPath(path);
				
				//设置transfomerClassesOrder
				String _orderPath = (path+"|"+transformerClass.getName()).replace(".", "//");
				//同一个path和TransformerClass的组合是唯一的,不允许重复
				if(sourceOrders.containsKey(_orderPath)) throw new RuntimeException("path:"+path+"/TransformerClass:"+transformerClass+" must be unique");
				sourceOrders.put((path+"|"+transformerClass.getName()).replace(".", "//"), source.isOrderAsPrevious()?sourceOrder:++sourceOrder);
				//添加到job的参数集,json的属性名成不能有.实际上path也是不能重复的,上面的path和TransformerClass的组合不能重复是不准确的
				jobParameters.addOrUpadte(".", path.replace(".", "//"), parameters);
				
				//设置到hadoop默认的机制中去
				MultipleInputs.addInputPath(hadoopJob, new Path(path), inputFormatClass, InjectionMapper.class);
			}
		};
		
		
		for(int i=0;i<sources.size();i++){
			Source source = sources.get(i);
			Class transformerClass = transfomerClasses.get(i);
			func.apply(source, transformerClass);
		}
		
		//覆盖MultipleInputs 89行,增加mapper的注入功能
		hadoopJob.setMapperClass(DelegatingMapperHack.class);
		
		//将job的参数集设置到conf中去
		jobConf.set("xs.hadoop.piece.iterated.jobParameters", jobParameters.toString());
		
		//设置transfomerClassesOrder---回来请修改成source order ?????
		jobConf.set("xs.hadoop.piece.iterated.transfomerClassesOrder", new Json(sourceOrders).toString());
		
		//需要从mapOutputClass获得,并映射到hadoop类型,hadoop类型的出装箱工作由框架来完成
		hadoopJob.setMapOutputKeyClass(IteratedUtil.toHadoopWritableComparableClass(mapOutputKeyClass[0]));
		
		if(joinerClass!=null){
			//输出必须是这个类型,规范了编程接口,但却降低了效率,增加属性_order
			hadoopJob.setMapOutputValueClass(MapWritable.class);
		}else{
			hadoopJob.setMapOutputValueClass(Text.class);
		}
		
		
		//最后肯定是要放入hdfs文件中,写入hbase的直接写在reducer中
		hadoopJob.setOutputKeyClass(Text.class);
		hadoopJob.setOutputValueClass(Text.class);
		
		//没有需要就设置为null,是否有文件输出路径和是否有reducer没有关系，只有mapper也可能有文件格式的输出
		if(outputPath!=null){
			//path要提前分配出来,并验证,不要到了这里再出错误,整个业务完成后中间目录要清理,保留最后结果
			TextOutputFormat.setOutputPath(hadoopJob, new Path(IteratedUtil.rightSlash(IteratedUtil.getBaseHdfsUri(),false)+IteratedUtil.leftSlash(outputPath,true)));
			//设置输出的格式
			jobConf.set("xs.hadoop.piece.iterated.Joiner.fieldNamesOrder", new Json(fieldNamesOrder).toString());
			
		}else{
			hadoopJob.setOutputFormatClass(NullOutputFormat.class);
		}
		
		if(joinerClass!=null){
			//设置joiner class
			jobConf.set("xs.hadoop.piece.iterated.Joiner.className", joinerClass.getName());
			//transformer的方式不一样
			if(driverParams!=null) jobConf.set("xs.hadoop.piece.iterated.Joiner.driverParams", driverParams.toString());
			
			//InjectionReducer默认的实现原理是取得xs.hadoop.piece.iterated.Joiner.className,实例化并注入
			hadoopJob.setReducerClass(InjectionReducer.class);
			
			hadoopJob.setNumReduceTasks(numOfReduceProcess);
		}
		
		//设置driver ip地址(改成计算机名更好一些，其他框架应该都是这样的)
		jobConf.set("xs.hadoop.piece.iterated.driverIP",IteratedUtil.getLoalIpHostName()[0]);
		
		hadoopJob.waitForCompletion(true);
		
		lastRunPath = outputPath;
	}
	
	
	String lastRunPath;
	
	public String getLastRunPath() {
		return lastRunPath;
	}
	
	
	/**
	 * 辅助函数式接口 
	 */
	static interface Function{
		void apply(Source source,Class transformerClass) throws Exception;
	}

	
	
	
}







