package xs.hadoop.iterated.input;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import jsr166y.ForkJoinPool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

import xs.hadoop.Json;
import xs.hadoop.iterated.Collector;
import xs.hadoop.iterated.Collector.Entry;
import xs.hadoop.iterated.IteratedUtil;
import xs.hadoop.iterated.JsonParams;
import xs.hadoop.iterated.input.HBase.HBaseCell;
import xs.hadoop.iterated.input.HBase.HBaseTransformer;
import xs.hadoop.iterated.netty.MessageClient;

/**
 *
 */
public class InjectionMapper<K0,V0> extends Mapper<K0,V0,WritableComparable,Writable> implements JsonParams {

	private Transformer transformer;
	
	private int sourceOrder;
	
	private int parallelism = 2;
	
	private String scheduleName;
	
	private String driverIP;
	
	private String   joinerClass; 
	private String[] fieldNamesOrder;
	
    ForkJoinPool forkJoinpool;
	protected void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
		
		scheduleName = context.getJobName().split("/")[1];
		Configuration conf = context.getConfiguration();
		driverIP = conf.get("xs.hadoop.piece.iterated.driverIP");
		
		//参数的传递问题,从main,source --> transformer和joiner的问题 ???
		joinerClass = conf.get("xs.hadoop.piece.iterated.Joiner.className");
		
		//当没有joiner(reducer)的时候,mapper就要输出结果
		if(joinerClass==null){
			//恢复fieldNamesOrder
			String jsonStr = conf.get("xs.hadoop.piece.iterated.Joiner.fieldNamesOrder");
			List<Json> jsonList= Json.desierialize(jsonStr).asList();
			if(jsonList.size()>0){
				fieldNamesOrder = new String[jsonList.size()+1];
				int i;
				for(i=0;i<fieldNamesOrder.length;i++){
					fieldNamesOrder[i] = jsonList.get(i).toString();
				}
				//当mapper输出的时候,在最后添加_order列
				fieldNamesOrder[i] = "_order";
			}else{
				//支持默认的每行单值的情况,在最后添加_order列
				fieldNamesOrder = new String[]{"_","_order"};
			}
		}
	
		
		try {
			//取得Transformer类型
			String transformerClassName = jsonParams.find("transformerClassName").toString();
			Class<? extends Transformer> transformerClass = (Class<? extends Transformer>) conf.getClassByName(transformerClassName);
			
			//完成注入
			transformer =  ReflectionUtils.newInstance(transformerClass, null);
			
			String path = jsonParams.find("path").toString();
			
			//取得order
			Json transfomerClassesOrder = Json.desierialize(conf.get("xs.hadoop.piece.iterated.transfomerClassesOrder"));
			sourceOrder = Integer.parseInt(transfomerClassesOrder.find((path+"|"+transformerClassName).replace(".", "//")).toString());
			
			//创建一个不需要join的pool
			forkJoinpool = new ForkJoinPool(parallelism,ForkJoinPool.defaultForkJoinWorkerThreadFactory,null,true);
			
			transformer.sourceParams = jsonParams.find("sourceInitParams.sourceParams");
			transformer.dirverParams = jsonParams.find("driverParams");
			transformer.sourceOrder  = sourceOrder;
			
			transformer.setup(createTransfomerJsonParam(conf));
			
		} catch (Exception e) {
			e.printStackTrace();
			IteratedUtil.reportException(e, "/"+scheduleName,driverIP);
			throw new RuntimeException(e);
		} 
		
	}
	
	/**
	 * 这样并不好,最好所有参数一窝全端过去
	 */
	private Json createTransfomerJsonParam(Configuration conf){
		Json jsonParam = Json.createJson();
		jsonParam.addOrUpadte(".", "driverIP",     new Json(driverIP));
		jsonParam.addOrUpadte(".", "scheduleName", new Json(scheduleName));
		jsonParam.addOrUpadte(".", "lastOutputfieldNamesOrder", new Json(conf.get("xs.hadoop.piece.iterated.lastOutputfieldNamesOrder")));
		return jsonParam;
	}

	protected void map(K0 key, V0 value, Context context) throws IOException, InterruptedException {
		//抛出以前出现的异常
		if(lastException!=null) throw new RuntimeException(lastException);
		
		/*-----------将hadoop类转化为基本的java类型 -------------*/
		
		//hbase有自己的处理方式,这个是特别的,最后由HBaseTransformer处理
		if((key instanceof ImmutableBytesWritable) && (value instanceof Result)){
			String  rowKey =  Bytes.toString(((ImmutableBytesWritable)key).copyBytes());
			//通过实际观察,当hbase作为source,key每次都是一样的,而value每次都是不同的
			callTransformerAsync(rowKey,value,context);
		}
		//转换成java数据类型,使得核心业务类可以使用最基本的java数据类型来工作
		else{
			Object javaKey   = IteratedUtil.toJavaObject(key);
			Object javaValue = IteratedUtil.toJavaObject(value);
			callTransformerAsync(javaKey, javaValue,context);
		}
	}
	
	/**
	 * 上一次执行时出现的异常
	 */
	private Exception lastException;
	
	private void callTransformerAsync(final Object key,final Object row,final Context context) throws InterruptedException{
		//防止过度消耗内存
		if(forkJoinpool.getQueuedSubmissionCount()>2000) Thread.sleep(1);
		
		forkJoinpool.submit(new Runnable() {
			public void run() {
				try {
					Collector<Object> collector = new Collector();
					if(row instanceof Result){
						//如果要使用多版本的数据，请继承HBaseTransformerForAllVersion，另外，还需要在conf里设置版本数量
						if(transformer instanceof HBaseTransformer){
							Map<String, String> _row = HBaseCell.getRowForLastVersion((Result)row);
							transformer.flatMap(key, _row, collector);
						}else{
							Map<String, List<HBaseCell>> _row = HBaseCell.getRowForAllVersions((Result)row);
							transformer.flatMap(key, _row, collector);
						}
					}else{
						transformer.flatMap(key, row, collector);
					}
					
					List<Entry<Object, Map<String, Object>>> rows = collector.get();
					
					if(rows!=null) writeContext(rows,context);
				} catch (Exception e) {
					e.printStackTrace();
					IteratedUtil.reportException(e, "/"+scheduleName,driverIP);
					lastException = e;
				} 
			}
		});
	}

	private static final String orderStr = "_order";
	private void writeContext(List<Entry<Object,Map<String, Object>>> rows,Context context) throws IOException, InterruptedException{
		
		//Entry中key,不是WritableComparable类型,这个注意一下
		for(Entry<Object, Map<String, Object>> entry:rows){
			
			WritableComparable  key1 = IteratedUtil.toHadoopWritableComparable(entry.getKey());
			Map<String, Object> rowValue = entry.getValue();
			
			//将自己的顺序加入,使得结果在reducer中是按照mapper聚集的
			if(!rowValue.containsKey(orderStr)) rowValue.put(orderStr, sourceOrder);
			MapWritable mapw = (MapWritable) IteratedUtil.toHadoopWritable(rowValue);
			
			if(joinerClass!=null) write(key1,mapw,context);     //存在reducer的情况
			else                  write(key1,rowValue,context); //不存在reducer的情况 
		}
	}
	
	/**
	 * 只是为了context.write同步,整个程序最终还是会慢在这里,提高并发的进程数可能是最有效的
	 * 创建一个特殊的集合类,其中包含着很多双端队列,每个队列绑定一个线程,write方法单独一个线程,循环从每个队列的头取数
	 * 创建一个方法判断这个特殊的集合是否都读完了,方法为便利所有队列,有一个size不是0就返回false????
	 */
	private synchronized void write(WritableComparable key1,MapWritable mapw,Context context) throws IOException, InterruptedException{
		context.write(key1, mapw);
	}
	
	private synchronized void write(WritableComparable key1,Map<String, Object> rowValue,Context context) throws IOException, InterruptedException{
		context.write(key1, new Text(format(rowValue)));
	}
	
	private String format(Map<String, Object> row){
		StringBuilder sb = new StringBuilder();
		for(int i=0;i<fieldNamesOrder.length;i++){
			Object fieldValue = row.get(fieldNamesOrder[i]);
			String text = fieldValue==null?"":String.valueOf(fieldValue);
			sb.append((sb.length()>0?"\t":"")+text);
		}
		
		return sb.toString();
	}
	
	
	protected void cleanup(Context context)	throws IOException, InterruptedException {
		try {
			forkJoinpool.shutdown();
			//很少有执行一天的情况
			forkJoinpool.awaitTermination(1, TimeUnit.DAYS);
			
			//最后一波抛出异常
			if(lastException!=null) throw new RuntimeException(lastException);
			
			//最后union的部分
			Collector<Object> collector = new Collector();
			try {
				transformer.union(collector);
			} catch (Exception e) {
				e.printStackTrace();
				IteratedUtil.reportException(e, "/"+scheduleName,driverIP);
				throw new RuntimeException(e);
			}
			List<Entry<Object, Map<String, Object>>> rows = collector.get();
			if(rows!=null) writeContext(rows,context);
		} finally{
			//销毁消息发送客户端
			MessageClient.close();
			super.cleanup(context);
		}
	}
	
	/*-------------------------------json------------------------------*/
	private Json  jsonParams;

	public void setJsonStr(String jsonStr) {
		this.jsonParams = Json.desierialize(jsonStr);
	}

	public String getJsonStr() {
		return jsonParams.toString();
	}
	
	public Json getJson() {
		return jsonParams;
	}
	
	
}





