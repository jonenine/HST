package xs.hadoop.iterated.output;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import jsr166y.ForkJoinPool;
import jsr166y.RecursiveTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

import xs.hadoop.Json;
import xs.hadoop.iterated.Collector;
import xs.hadoop.iterated.Collector.Entry;
import xs.hadoop.iterated.IteratedUtil;
import xs.hadoop.iterated.netty.MessageClient;

public class InjectionReducer extends Reducer<WritableComparable,MapWritable,Text,Text> {
	
	Joiner joiner;
	String[] fieldNamesOrder;
	
    int parallelism = 4;
	
    ForkJoinPool forkJoinpool;
	
	static int batchSize = 200;
	//可能远远达不到,应该一直是2
	static int MaxBatchNum = 10;
	
	
	/*----------------------------保证多线程处理后,按照reduce顺序写入context--------------------------*/
	
	/**
	 * 保持顺序,控制大小,过大阻塞
	 */
	private ArrayBlockingQueue<BatchResult> batchQueue;
	
	/**
	 * 保证当一个batch完成后,会tryOutput一次
	 * 框架的算法是每完成一个batch,就tryOutput一次,这样能保证所有的完成的batch可以按顺序及时的output
	 */
	private LinkedBlockingQueue<Integer> numOutPutRequest = new LinkedBlockingQueue<Integer>();
	
	
	private void tryOutput(){
		BatchResult firstResult;
		//便利batchQueue,从head开始,查询full的batch并output,直到没有full的batch
		do{
			firstResult = batchQueue.peek();
			if(firstResult!=null && firstResult.isFull) {
				firstResult.output();
				batchQueue.poll();
			}
			
			else break;
		}while(true);
	}
	
	/**
	 * 这个线程应该独立出来,不能用forkjoinpool执行,否则会造成间接的死锁
	 */
	private Thread outPutThread = new Thread(){
		public void run() {
			try {
				//阻塞,而且当返回0的时候循环结束
				while(numOutPutRequest.take()!=0) tryOutput();
			} catch (InterruptedException e) {}
		}
	};
	
	private static final Integer s1 =1;
	private void requestOutPut(){
		numOutPutRequest.add(s1);
	}
	
	/**
	 * 当任务都完成后,不会有新的任务提交,就请求output线程停止,这样output线程处理了所有的没有处理的请求之后,就会停止下来
	 * 当output线程结束后,方法返回
	 */
	private void closeAndJoinOutPut() throws InterruptedException{
		//异步让线程停止
		numOutPutRequest.add(0);
		//等待线程结束
		outPutThread.join();
	}
	
	private String scheduleName;
	private String driverIP;
	
    protected void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
		
		scheduleName = context.getJobName().split("/")[1];
		Configuration conf = context.getConfiguration();
		driverIP = conf.get("xs.hadoop.piece.iterated.driverIP");
		try {
			//生成 joiner 实例
			String joinerClassClass = conf.get("xs.hadoop.piece.iterated.Joiner.className");
			Class<? extends Joiner> joinerClass = (Class<? extends Joiner>)conf.getClassByName(joinerClassClass);
			joiner = ReflectionUtils.newInstance(joinerClass, null);
			
			//恢复fieldNamesOrder
			String jsonStr = conf.get("xs.hadoop.piece.iterated.Joiner.fieldNamesOrder");
			List<Json> jsonList= Json.desierialize(jsonStr).asList();
			fieldNamesOrder = new String[jsonList.size()];
			for(int i=0;i<jsonList.size();i++){
				fieldNamesOrder[i] = jsonList.get(i).toString();
			}
			
			//支持默认的每行单值的情况
			if(fieldNamesOrder.length == 0) fieldNamesOrder = new String[]{"_"};
			
			//这个需要join
			forkJoinpool = new ForkJoinPool(parallelism);
			
			batchQueue = new ArrayBlockingQueue(MaxBatchNum);
			
			//启动output线程
			outPutThread.start();
			
			String driverParamsStr = conf.get("xs.hadoop.piece.iterated.Joiner.driverParams");
			if(driverParamsStr!=null) joiner.dirverParams = Json.desierialize(driverParamsStr);
			
			joiner.setup(createJoinerJsonParam());
			
			joiner.shouldRegisterTimer();
			joiner.couldRegisterTimer = false;
			
			routineThread.start();
			
		} catch (Exception e) {
			e.printStackTrace();
			IteratedUtil.reportException(e, "/"+scheduleName,driverIP);
			throw new RuntimeException(e);
		}
	}
    
    
    private Json createJoinerJsonParam(){
    	Json jsonParam = Json.createJson();
		jsonParam.addOrUpadte(".", "driverIP", new Json(driverIP));
		jsonParam.addOrUpadte(".", "scheduleName", new Json(scheduleName));
		return jsonParam;
    }
    
    private class BatchResult{
    	
    	List<Entry<Object, Map<String, Object>>>[] rowses;
    	
		BatchResult(int batchSize){
			rowses = new List[batchSize];
		}
		
		int addResult(int i,List<Entry<Object, Map<String, Object>>> result){
			rowses[i] = result;
			return result.size();
		}
		
		//是否完整
		transient boolean isFull = false;
		
		int output(){
			int outputSize = 0;
			for(List<Entry<Object, Map<String, Object>>> rows:rowses){
				try {
					writeContext(rows);
					outputSize+=rows.size();
				} catch (Exception e) {
					e.printStackTrace();
					lastException = e;
					throw new RuntimeException(e);
				} 
			}
			
			return outputSize;
		}
	}

	class BatchTask extends RecursiveTask<Void>{
		Object[][]  keyHandlers;
		BatchResult batchResult;
		public BatchTask(Object[][] keyHandlers) throws InterruptedException {
			this.keyHandlers = keyHandlers;
			batchResult = new BatchResult(keyHandlers.length);
			//创建时就添加到队列，保证同reduce的顺序一致
			batchQueue.put(batchResult);
		}

		protected Void compute() {
			OneTask[] tasks = new OneTask[keyHandlers.length];
			for(int i=0;i<tasks.length;i++){
				Object[] keyHandler = keyHandlers[i];
				OneTask task = new OneTask(keyHandler[0],(RowHandler) keyHandler[1],i);
				
				task.fork();
				tasks[i] = task;
			}
			
			for(int i=0;i<tasks.length;i++) batchResult.addResult(i,tasks[i].join());
			
			batchResult.isFull = true;
			
			//当一个batch结束的时候,请求output一次,框架会保证异步的执行一次
			requestOutPut();
			
			return null;
		}
		
	}
	
	
	class OneTask extends RecursiveTask<List>{
		Object      key;
		RowHandler  handler;
		int         indexInBatch;
		
		public OneTask(Object key, RowHandler handler,int indexInBatch) {
			this.key = key;
			this.handler = handler;
			this.indexInBatch = indexInBatch;
		}

		protected List compute() {
			try {
				//ThreadLocal的东西是不能再交给其他线程的,必须在一个线程内(方法内)出来
				Collector collector = new Collector();
				//执行业务在这里
				joiner.join(key, handler ,collector);
				return collector.get();
			} catch (Exception e) {
				e.printStackTrace();
				IteratedUtil.reportException(e, "/"+scheduleName,driverIP);
				lastException = e;
			}
			return null;
		}
		
	}
	
	private Exception lastException;
	
	private Object[][] currentKeyHandlers;
	private int currentIndex = 0;
	
	private Context context;
    protected void reduce(WritableComparable key, Iterable<MapWritable> values,Context context) throws IOException, InterruptedException {
    	//这个注意,如果基类变了,这里要注意,不过应该没问题
		if(this.context==null) this.context = context;
		
		//抛出以前出现的异常
		if(lastException!=null) throw new RuntimeException(lastException);
				
    	//转换成java类型
    	List<List<Map<String,Object>>> groupedValues = IteratedUtil.group(values);
		RowHandler handler = new RowHandler(groupedValues);
		Object javaKey = IteratedUtil.toJavaObject(key);
		
		if(currentKeyHandlers == null) currentKeyHandlers = new Object[batchSize][];
		currentKeyHandlers[currentIndex++] = new Object[]{javaKey,handler};
		
		//完成一波,提交
		if(currentIndex == batchSize){
			forkJoinpool.submit(new BatchTask(currentKeyHandlers));
			
			currentKeyHandlers = null;
			currentIndex = 0;
		}
	}
    
    
	

	protected void cleanup(Context context) throws IOException,InterruptedException {
		try {
			//最后一波,不要忘记
			if(currentIndex!=0){
				//转换成最后一波实际的大小
				Object[][] _currentKeyHandlers = new Object[currentIndex][];
				System.arraycopy(currentKeyHandlers, 0, _currentKeyHandlers, 0, currentIndex);
				//提交
				forkJoinpool.submit(new BatchTask(_currentKeyHandlers));
				
				currentKeyHandlers = null;
				currentIndex = 0;
			}
			
			
			forkJoinpool.shutdown();
			//很少有执行一天的情况
			forkJoinpool.awaitTermination(1, TimeUnit.DAYS);
			
			//最后一波抛出异常
			if(lastException!=null) throw new RuntimeException(lastException);
			
			//等待output线程停止----下面这两行在放在finally(catch)之后也要放一下,保证输出不会在cleanup之后进行,否则mapreduce程序自己停不下来
			closeAndJoinOutPut();
			//最后扫尾输出,这里是同步的,异常可以直接抛出
			tryOutput();
			
			/*--------------------最后union的部分------------------*/
			Collector collector = new Collector();
			try {
				joiner.union(collector);
			} catch (Exception e) {
				e.printStackTrace();
				IteratedUtil.reportException(e, "/"+scheduleName,driverIP);
				throw new RuntimeException(e);
			}
			
			List<Entry<Object, Map<String, Object>>> rows = collector.get();
			if(rows!=null) writeContext(rows);
			
		}finally{
			try {
				super.cleanup(context);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			
			routineThread.interrupt();
			routineThread.join(10000);
			
			try {
				//最后再同步的执行一次
				runRoutineOnce(true);
			} catch (Exception e) {}
			
			
			//销毁消息发送客户端
			MessageClient.close();			
		}
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
	
	Text keyText   = new Text();
	Text valueText = new Text();
	public void writeContext(List<Entry<Object, Map<String, Object>>> rows) throws IOException, InterruptedException{
		
		for(Entry<Object, Map<String, Object>> row:rows){
			keyText.set(String.valueOf(row.getKey()));
			
			Map<String, Object> map = row.getValue();
			valueText.set(format(map));
			
			write(keyText,valueText);
		}
	}
	
	private synchronized void write(Text keyText,Text valueText) throws IOException, InterruptedException{
		context.write(keyText,valueText);
	}
	
	
	/*----------------------------定时器单独一个线程------------------------------*/
	
	private void runRoutineOnce(boolean isForce) throws Exception{
		long current = System.currentTimeMillis();
		Iterator<Joiner.Timer> it = joiner.timers.values().iterator();
		while(it.hasNext()){
			Joiner.Timer timer = it.next();
			if(isForce || (current - timer.lastTime) > timer.timeInteval){
				try {
					//定时器也是不能出错的
					timer.routine();
				} catch (Exception e) {
					e.printStackTrace();
					lastException = e;
				}
				timer.lastTime = current;
			}
			Thread.sleep(2);
		}
	}
	
	Thread routineThread = new Thread(new Runnable() {
		public void run() {
			if(joiner.timers != null){
				try {
					while(true){
						runRoutineOnce(false);
						Thread.sleep(100);
					}
				} catch (Exception e) {}
			}
		}
	});

}



