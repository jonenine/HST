package test.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import xs.hadoop.Json;
import xs.hadoop.iterated.Collector;
import xs.hadoop.iterated.IteratedJob;
import xs.hadoop.iterated.IteratedUtil;
import xs.hadoop.iterated.Scheduler;
import xs.hadoop.iterated.input.FountainNames;
import xs.hadoop.iterated.input.Source;
import xs.hadoop.iterated.input.Transformer;
import xs.hadoop.iterated.output.Joiner;
import xs.hadoop.iterated.output.RowHandler;

/**
 * 本地文件测试
 */
@SuppressWarnings("unchecked")
public class LocalFileTester {

	public static void main(String[] args) throws Exception{
		//依赖hdfs上的一些文件夹,比如输出路径等
		IteratedUtil.setBaseHdfsUri("hdfs://172.16.144.132:8020/");
		
		Scheduler scheduler = new Scheduler("testSchedule",args);
		
        IteratedJob<Long> iJob = scheduler.createJob("testJob")
        		//将fountainName收集到IteratedJob中,然后将fountainName和index关系放入configuaration中,在Transformer可以覆盖fountainName
        		//在Joiner中使用fountainName代替index来取得数据,实际计算过程使用index
				.from(Source.textFile("file:///home/cdh/0.txt").setParam("a", "aa"),Transformer0.class).as("left")
				.from(Source.textFile("file:///home/cdh/1.txt"),Transformer0.class).as("right")
				.join(Joiner0.class);
				//必须指定列名数组,建立数据模型,并指定format方式,每行单列的情况下,不用设置,设置反而错误
				//.format("business");
		scheduler.arrange(iJob);
		
		scheduler.run();
		
		/**
		 * 都完成之后销毁资源,否则进程退不出去,如果是以服务器方式工作是不用写的
		 */
		IteratedUtil.destoryResource();
	}
	
	public static class Transformer0 extends Transformer<Long, String, Long> {

		public Class<Long> getMapOutputKeyClass() {
			return Long.class;
		}
		
		public void flatMap(Long inputKey, String inputValue,Collector<Long> collector) throws Exception{
			System.out.println(this.getSourceOrder()+"-------------------------------------------"+this.getSourceParam("a"));
			System.out.println(this.getSourceOrder()+"--------------------driver--------------------"+this.getDriverParam(0));
			
			String[] values = inputValue.split("\t");
			
			/**
			 * 在一次flatMap中,只能使用一种模式,不能混用
			 */
			//1.单值模式
			collector.singleValueRow(Long.parseLong(values[0]), values[1]);
			
			//2.单行模式,只是收集不同的属性,效率比较高
			if(1==2){
				collector.uniqueRow(Long.parseLong(values[0])).setField("business",values[1]);
			}
			
			//3.多行模式,可以返回多行
			if(1==2){
				Map<String, Object> row = new HashMap();
				row.put("value", values[1]);
				
				collector.row(Long.parseLong(values[0]), row);
			}
		}

		public void union(Collector<Long> collector) throws Exception{
			
		}
	}
	
	public static class Transformer1 extends Transformer<Long, String, Long> {

		public Class<Long> getMapOutputKeyClass() {
			return Long.class;
		}

		public void flatMap(Long inputKey, String inputValue,xs.hadoop.iterated.Collector<Long> collector) throws Exception{
			
			String[] values = inputValue.split("\t");
			
			/**
			 * 在一次flatMap中,只能使用一种模式,不能混用
			 */
			
			collector.singleValueRow(Long.parseLong(values[0]), values[1]);
			
			if(1==2){//单行模式,只是收集不同的属性,效率比较高
				collector.uniqueRow(Long.parseLong(values[0])).setField("business",values[1]);
			}
			
			if(1==2){//多行模式,可以返回多行
				Map<String, Object> row = new HashMap();
				row.put("value", values[1]);
				
				collector.row(Long.parseLong(values[0]), row);
			}
		}

		public void union(xs.hadoop.iterated.Collector<Long> collector) throws Exception{
			
		}		
	}
	
	
	/**
	 * 范型的第一个类型必须和Transformer的最后一个一样,否则的会报类型转换错误
	 */
	public static class Joiner0 extends Joiner<Long, String, String> {
		

		public void shouldRegisterTimer() {
			this.registerCounter("counter0");
			this.registerCounter("计数器2");
		}

		public void join(Long key,RowHandler handler,Collector collector) throws Exception{
			 List<Object> row = handler.getSingleFieldRows(0);
			 List<Object> row2 = handler.getSingleFieldRows(1);
			 System.out.println("-------------"+key+"/"+row+"/"+row2+"---------"+this.getDriverParam(1));
			
			 //没有解决报错后,mapper和reducer进程无法退出的问题
			 //Long.parseLong("aaaa");
			 
			 //collector.uniqueRow(4-key).setField("business", row.toString());
			 collector.singleValueRow(4-key, row+"/"+row2);
			 
			 this.getCounter("counter0").incrementAndGet();
			 this.getCounter("计数器2").addAndGet(7);
			 Thread.sleep(4000);
		}

		public void union(Collector collector) throws Exception{
			
		}
	}

}


