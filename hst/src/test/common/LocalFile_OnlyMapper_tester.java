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
 * 只有reduce没有join
 */
@SuppressWarnings("unchecked")
public class LocalFile_OnlyMapper_tester {

	public static void main(String[] args) throws Exception{
		IteratedUtil.setBaseHdfsUri("hdfs://172.16.144.132:8020/");
		
		Scheduler scheduler = new Scheduler("testSchedule",args);
		
        IteratedJob<Long> iJob = scheduler.createJob("testJob")
				.from(Source.textFile("file:///home/cdh/0.txt"),Transformer0.class).as("left")
				.from(Source.textFile("file:///home/cdh/1.txt"),Transformer1.class).as("right");
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

	}
	

}


