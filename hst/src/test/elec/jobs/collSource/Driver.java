package test.elec.jobs.collSource;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import xs.hadoop.HTableAO;
import xs.hadoop.Json;
import xs.hadoop.iterated.Collector;
import xs.hadoop.iterated.IteratedJob;
import xs.hadoop.iterated.Scheduler;
import xs.hadoop.iterated.input.HBase.HBaseCell;
import xs.hadoop.iterated.input.HBase.HBaseTransformerForAllVersion;
import xs.hadoop.iterated.output.Joiner;
import xs.hadoop.iterated.output.RowHandler;

/**
 * 前置机报文统计 
 */
public class Driver extends ElecDriver {
	
	private static SimpleDateFormat yyMMdd = new SimpleDateFormat("yyMMdd");
	
	/**
	 * 因为TableName需要写死，不再使用spring配置
	 */
	private static HTableAO getT_SCRIPT_HIS_AO() throws Exception{
		HTableAO hTableAo = new HTableAO();
		hTableAo.setZookeeperQuorum("10.120.209.60,10.120.209.49,10.120.209.48");
		hTableAo.setTableName("T_SCRIPT_HIS");
		hTableAo.afterPropertiesSet();
		
		return hTableAo;
	}
	
	/**
	 * main里也可以放多个程序，通过shell来指定程序名称 
	 */
	public static void main(String[] args) throws Exception{ 
		try {
			init();
			Scheduler scheduler = new Scheduler("epmisBDSchedule",args);
			
			//昨天是哪一天这一参数由driver传进来,注意标准格式为160504
			String yesDay = scheduler.getDriverParam(0);
			//yesDay = "160501";
			System.out.println("driver输入参数："+yyMMdd.parse(yesDay));
			
			HTableAO hTableAo = getT_SCRIPT_HIS_AO();
			String hBasekey = "0_S_D_"+yesDay;
			hTableAo.delete(hBasekey);
			hTableAo.flushMemStore();
			hTableAo.close();
			
			IteratedJob<Long> collSource_StasJob = scheduler.createJob("collSource_Stas")
					.from(getEpmisPartitionSources("T_SCRIPT_HIS",yyMMdd.parse(yesDay)), CountTransformer.class)
					.join(CountJoiner.class).parallelism(1, 6);
			
			scheduler.arrange(collSource_StasJob);
			
			scheduler.run();
		}catch (Exception e) {
			e.printStackTrace();
		}
			
		destroy();
	}//~main
	
	
	public static class CountTransformer extends HBaseTransformerForAllVersion<String>{

		public Class<String> getMapOutputKeyClass() {
			return String.class;
		}
		
		public void setup(Json jsonParam) throws Exception {
			super.setup(jsonParam);
			//在独立于driver和worker的另外一台机器上执行
			this.reportDriver("-----"+this.sourceOrder);
		}

		ConcurrentHashMap<String, AtomicLong> countMap = new ConcurrentHashMap<String, AtomicLong>();

		public void flatMap(String inputKey,Map<String, List<HBaseCell>> inputValue,Collector<String> collector) throws Exception {
			for(List<HBaseCell> cells:inputValue.values()){
				for(HBaseCell cell:cells){
					//direction+"_"+length+"_"+hms+"_"+machine
					String[] infos   = cell.getValue().split("\\_");
					String direction = infos[0];
					String length    = infos[1];
					long increment   = Long.parseLong(length);
					//012428
					String hms       = infos[2];
					//得到小时
					int h = Integer.parseInt(hms.substring(0, 2));
					String machine   = infos[3];
					
					//每机器，方向，小时
					String key1 = machine+"_"+direction+"_"+h;
					if(!countMap.containsKey(key1)){
						AtomicLong counter = new AtomicLong(0);
						countMap.putIfAbsent(key1,counter);
					}
					countMap.get(key1).addAndGet(increment);
					
					//每方向，小时
					String key5 = direction+"_"+h;
					if(!countMap.containsKey(key5)){
						AtomicLong counter = new AtomicLong(0);
						countMap.putIfAbsent(key5,counter);
					}
					countMap.get(key5).addAndGet(increment);
					
					//每方向
					String key6 = direction;
					if(!countMap.containsKey(key6)){
						AtomicLong counter = new AtomicLong(0);
						countMap.putIfAbsent(key6,counter);
					}
					countMap.get(key6).addAndGet(increment);
					
					
					//每机器，方向(汇总)
					String key2 = machine+"_"+direction;
					if(!countMap.containsKey(key2)){
						AtomicLong counter = new AtomicLong(0);
						countMap.putIfAbsent(key2,counter);
					}
					countMap.get(key2).addAndGet(increment);
					
					//每机器
					String key3 = machine;
					if(!countMap.containsKey(key3)){
						AtomicLong counter = new AtomicLong(0);
						countMap.putIfAbsent(key3,counter);
					}
					countMap.get(key3).addAndGet(increment);
					
					//所有的
					String key4 = "sum";
					if(!countMap.containsKey(key4)){
						AtomicLong counter = new AtomicLong(0);
						countMap.putIfAbsent(key4,counter);
					}
					countMap.get(key4).addAndGet(increment);
				}//~for
			}//~for
		}//~flatmap

		public void union(Collector<String> collector) throws Exception {
			for(String key:countMap.keySet()){
				collector.singleValueRow(key, countMap.get(key).get());
			}
		}
	}
	
	public static class CountJoiner extends Joiner<String, String, Long> {
		
		HTableAO hTableAo;
		
		public void setup(Json jsonParam) throws Exception {
			super.setup(jsonParam);
			
			hTableAo = getT_SCRIPT_HIS_AO();
		}
		
		//注意所有的集合都要加同步
		List<String[]> result = Collections.synchronizedList(new LinkedList<String[]>());
		public void join(String key, RowHandler handler, Collector collector) throws Exception {
			List<Object> rows = handler.getSingleFieldRows(0);
			long count = 0;
			if(rows!=null){
				for(Object _value:rows){
					count +=Long.parseLong(_value.toString());
				}
				result.add(new String[]{key,String.valueOf(count)});
			}
			
			//collector.singleValueRow(key, count);
		}
		public void union(Collector collector) throws Exception {
			super.union(collector);
			
			//写入hbase
			String yesDay   = this.getDriverParam(0);
			//插入hbase，不要浪费每一个region
			String hBasekey = "0_S_D_"+yesDay;
			
			//this.reportDriver(hBasekey+"||||||"+new Json(result).toString());
			
			hTableAo.hbaseInsert(hBasekey, result.toArray(new String[result.size()][]));
			hTableAo.flushMemStore();
			hTableAo.close();
		}
		
		
	}
	
}











