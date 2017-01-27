package xs.hadoop.iterated.output;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import xs.hadoop.Json;
import xs.hadoop.iterated.Collector;
import xs.hadoop.iterated.IteratedUtil;

public abstract class Joiner<K1,K2,V2> {
	
	protected Json jsonParam;
	
	private String scheduleName;
	private String driverIP;
	
	/**
	 * 比如一个reducer,最后只是将结果写入hbase或其他存储,这里做个客户端初始化工作,不推荐使用outputformat
	 */
	public void setup(Json jsonParam) throws Exception{
		this.jsonParam = jsonParam;
		this.scheduleName = jsonParam.find("scheduleName").toString();
		this.driverIP     = jsonParam.find("driverIP").toString();
	}
	
	/*--------------------------------------------------------------*/
	protected Json dirverParams;
	
	/**
	 * 得到driver main的参数 
	 */
	public String getDriverParam(int pIndex){
		if(dirverParams!=null){
			Json param = dirverParams.find("arg"+pIndex);
			return param == null?null:param.toString();
		}
		return null;
	}
	
	
	/*--------------------------------------------------------------*/
	
	public void reportDriver(Exception e){
		IteratedUtil.reportException(e, "/"+scheduleName,driverIP);
	}
	
	public void reportDriver(String message){
		IteratedUtil.reportMessage(message, "/"+scheduleName,driverIP);
	}
	
	/*--------------------------------------------------------------*/
	
	public Map<String,Joiner.Timer> timers;
	
	public static abstract class Timer{
		public long lastTime;

		int timeInteval;
		String name;
		
		public Timer(String name,int timeInteval){
			this.name = name;
			this.timeInteval = timeInteval;
			lastTime = System.currentTimeMillis();
		}
		
		public abstract void routine() throws Exception;
	}
	
	public void registerTimer(Joiner.Timer timer){
		if(!couldRegisterTimer) return;
		if(timers == null) timers = new HashMap<String,Joiner.Timer>();
		timers.put(timer.name,timer);
	}
	
	/*--------------------------------------------------------------*/
	boolean couldRegisterTimer = true;
	
	/**
	 * 子类应该继承
	 */
	public void shouldRegisterTimer(){ }
	
	public class Counter extends Timer{
		AtomicLong sum = new  AtomicLong(0);
		
		public AtomicLong getSum() {
			return sum;
		}

		public Counter(String name,int timeInteval) {
			super(name, timeInteval);
		}

		public void routine() throws Exception{
			long _sum = this.sum.get();
			reportDriver("counter:"+this.name+":"+_sum);
		}
	}
	
	public void registerCounter(String name){
		registerTimer(new Counter(name,1000));
	}
	
	public AtomicLong getCounter(String name){
		Timer timer = timers.get(name);
		if(timer!=null && timer instanceof Joiner.Counter) return ((Joiner.Counter)timer).getSum();
		return null;
	}
	
	/**
	 * 使用hadoopUtil的group方法进行灵活的分组,map的转换也需要大量时间,考虑在reducer中也使用多线程的
	 * @param key
	 * @param values1
	 * @return
	 */
	public abstract void join(K1 key,RowHandler handler,Collector collector)throws Exception;
	
	public void union(Collector collector) throws Exception{};
	
}










