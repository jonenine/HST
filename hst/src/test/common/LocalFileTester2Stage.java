package test.common;

import java.util.List;

import xs.hadoop.iterated.Collector;
import xs.hadoop.iterated.IteratedJob;
import xs.hadoop.iterated.IteratedUtil;
import xs.hadoop.iterated.Scheduler;
import xs.hadoop.iterated.input.PreviousResultTransformer;
import xs.hadoop.iterated.input.Source;
import xs.hadoop.iterated.input.Transformer;
import xs.hadoop.iterated.output.Joiner;
import xs.hadoop.iterated.output.RowHandler;

/**
 * 本地文件测试
 * 多次迭代
 */
@SuppressWarnings("unchecked")
public class LocalFileTester2Stage {

	public static void main(String[] args) throws Exception{
		try {
			IteratedUtil.setBaseHdfsUri("hdfs://172.16.144.132:8020/");
			
			Scheduler scheduler = new Scheduler("testSchedule",args);
			
			IteratedJob<Long> iJob = scheduler.createJob("testJob")
					.from(Source.textFile("file:///home/cdh/0.txt"),Transformer1_0.class)
					.from(Source.textFile("file:///home/cdh/1.txt"),Transformer1_0.class)
					.join(Joiner1_0.class)
					//必须指定列名数组,建立数据模型,并指定format方式,每行单列的情况下,不用设置,设置反而错误
					.format("f1","f2");
			scheduler.arrange(iJob);
			
			IteratedJob<Long> stage2Job = scheduler.createJob("stage2Job")
					.fromPrevious(iJob, Transformer2_0.class);
					
			scheduler.arrange(stage2Job);
			
			scheduler.run();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		IteratedUtil.destoryResource();
	}
	
	/**
	 * 如果source有多个fountain的,提前用元数据说明,主要是取得fountainName和其序列对应关系
	 */
	public static class Transformer1_0 extends Transformer<Long, String, Long> {

		public Class<Long> getMapOutputKeyClass() {
			return Long.class;
		}
		
		public void flatMap(Long inputKey, String inputValue,Collector<Long> collector) throws Exception{
			String[] values = inputValue.split("\t");
			collector.singleValueRow(Long.parseLong(values[0]), values[1]);
		}
	}
	
	/**
	 * 范型的第一个类型必须和Transformer的最后一个一样,否则的会报类型转换错误
	 */
	public static class Joiner1_0 extends Joiner<Long, String, String> {

		public void join(Long key,RowHandler handler,Collector collector) throws Exception{
			 List<Object> row = handler.getSingleFieldRows(0);
			 List<Object> row2 = handler.getSingleFieldRows(1);
			 collector.uniqueRow(4-key).setField("f1", row).setField("f2", row2);
		}

	}
	
	/*--------------------------------------stage2---------------------------------------*/
	
	public static class Transformer2_0_0 extends Transformer<Long, String, Long> {
		public Class<Long> getMapOutputKeyClass() {
			return Long.class;
		}

		public void flatMap(Long inputKey, String inputValue,Collector<Long> collector) throws Exception {
			System.out.println(inputKey+"/"+inputValue);
		}
	}
	
	public static class Transformer2_0 extends PreviousResultTransformer<Long> {
		public Class<Long> getMapOutputKeyClass() {
			return Long.class;
		}
		
		/**
		 * 但是这样会产生一个问题,就是多线程的flatMap会将已经排序好的上一个job的结果打乱
		 */
		public void flatMap(Long inputKey, String[] inputValues,Collector<Long> collector) {
			//每天电量,已乘ct,pt,经过一次mapReduce的结果应该都是字符串类型
			String f1 = getFiledValue(inputValues, "f1");
			String f2 = getFiledValue(inputValues, "f2");
			System.out.println(f1+"/"+f2);
			
		}
	}

}


