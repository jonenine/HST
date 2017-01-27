package test.common;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

import xs.hadoop.Json;
import xs.hadoop.iterated.Collector;
import xs.hadoop.iterated.IteratedJob;
import xs.hadoop.iterated.IteratedUtil;
import xs.hadoop.iterated.Scheduler;
import xs.hadoop.iterated.input.Source;
import xs.hadoop.iterated.input.Transformer;
import xs.hadoop.iterated.input.HBase.HBaseTransformer;
import xs.hadoop.iterated.input.HBase.HBaseTransformerForAllVersion;
import xs.hadoop.iterated.output.Joiner;
import xs.hadoop.iterated.output.RowHandler;

/**
 *  hbase和文本join的例子
 */
@SuppressWarnings("unchecked")
public class HBaseTester {

	public static void main(String[] args) throws Exception{
		//测试时需要,集群模式下写不写无所谓
		IteratedUtil.setBaseHdfsUri("hdfs://172.16.144.132:8020/");
		
		Scheduler scheduler = new Scheduler("testSchedule",args);
		
		
        //表1
        Configuration conf0 = HBaseConfiguration.create();
        conf0.set("hbase.zookeeper.property.clientPort", "2181");
        conf0.set("hbase.zookeeper.quorum", "172.16.144.132,172.16.144.134,172.16.144.136");
		
        conf0.set(TableInputFormat.INPUT_TABLE,"APPLICATION_JOBS");
        conf0.set(TableInputFormat.SCAN_COLUMN_FAMILY,"cf");
        conf0.set(TableInputFormat.SCAN_CACHEBLOCKS,"false");
        conf0.set(TableInputFormat.SCAN_BATCHSIZE,"20000");
        
        
        //表2
        Configuration conf1 = HBaseConfiguration.create();
        conf1.set("hbase.zookeeper.property.clientPort", "2181");
        conf1.set("hbase.zookeeper.quorum", "172.16.144.132,172.16.144.134,172.16.144.136");
		
        conf1.set(TableInputFormat.INPUT_TABLE,"E_MP_DAY_READ2");
        conf1.set(TableInputFormat.SCAN_ROW_START, "151119_0");
        conf1.set(TableInputFormat.SCAN_ROW_STOP,  "151119_0_50734350");
        //一定要设置列族
        conf1.set(TableInputFormat.SCAN_COLUMN_FAMILY,"cf");
        conf1.set(TableInputFormat.SCAN_CACHEBLOCKS,"false");
        conf1.set(TableInputFormat.SCAN_BATCHSIZE,"20000");
        
        
        IteratedJob<Long> iJob = scheduler.createJob("testJob")
        		.from(Source.hBase(conf0), TransformerForHBase0.class).parallelism(5)
				.from(Source.hBase(conf1), TransformerForHBase1.class).parallelism(10)
				.from(Source.textFile("file:///home/cdh/0.txt"),Transformer0.class)
				.join(JoinerHBase.class).parallelism(6,10);
				//必须指定列名数组,建立数据模型,并指定format方式
				//.format("business");
        
		scheduler.arrange(iJob);
		
		scheduler.run();
	}
	
	public static class Transformer0 extends Transformer<Long, String, Long> {

		public Class<Long> getMapOutputKeyClass() {
			return Long.class;
		}

		public void flatMap(Long inputKey, String inputValue,Collector<Long> collector) {
			String[] values = inputValue.split("\t");
			collector.singleValueRow(Long.parseLong(values[0]), values[1]);
		}

		public void union(Collector<Long> collector) {
			
		}
	}
	
	public static class TransformerForHBase0 extends HBaseTransformer<Long> {

		public Class<Long> getMapOutputKeyClass() {
			return Long.class;
		}

		public void union(Collector<Long> collector) {
			
		}
		
		AtomicLong count = new AtomicLong(0);
		public void flatMap(String key, Map<String, String> row,Collector<Long> collector) {
			row.put("key", key);
			collector.row(count.incrementAndGet(), row);
		}
	}

	
	public static class TransformerForHBase1 extends HBaseTransformer<Long> {

		public Class<Long> getMapOutputKeyClass() {
			return Long.class;
		}

		public void union(Collector<Long> collector) {
			
		}

		AtomicLong count = new AtomicLong(0);
		public void flatMap(String key, Map<String, String> row,
				Collector<Long> collector) {
			row.put("key", key);
			collector.row(count.incrementAndGet(), row);
		}

	}
	
	public static class JoinerHBase extends Joiner<Long, String, String> {

		public void join(Long key, RowHandler handler, Collector collector) {
			Map map0 = handler.getRow(0);
			Map map1 = handler.getRow(1);
			
			List<Object> map2 = handler.getSingleFieldRows(2);
			
			collector.singleValueRow(key, map0+"/"+map1+"/"+map2);
		}

		public void union(Collector collector) {
		}

	
	}
	
	
}




