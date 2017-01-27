package test.elec.jobs.collSource;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

import xs.hadoop.HTableAO;
import xs.hadoop.iterated.IteratedUtil;
import xs.hadoop.iterated.input.Source;

/**
 * 封装一些参数和常量，都和集群相关 
 */
public class ElecDriver {
	
	
	protected static void init(){
		IteratedUtil.setBaseHdfsUri("hdfs://10.120.209.48:8020");
	}
	
	protected static void destroy(){
		IteratedUtil.destoryResource();
	}
	
	public static HTableAO getHTableAO(String tableName) throws Exception{
		HTableAO hTableAo = new HTableAO();
		hTableAo.setZookeeperQuorum("10.120.209.60,10.120.209.49,10.120.209.48");
		hTableAo.setTableName(tableName);
		hTableAo.afterPropertiesSet();
		
		return hTableAo;
	}
	
	/**
	 * @param tableName
	 * @param startKey
	 * @param endKey
	 * @param scan_coloumns
	 * @return
	 */
	protected static Configuration getHBaseConfiguration(String tableName,String startKey,String endKey,String... scan_coloumns){
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum", "10.120.209.60,10.120.209.49,10.120.209.48");
		//亿力平台特有
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
	     
		conf.set(TableInputFormat.INPUT_TABLE,tableName);
		
		if(startKey!=null && (startKey=startKey.trim()).length()>0) conf.set(TableInputFormat.SCAN_ROW_START,startKey);
		if(endKey!=null && (endKey=endKey.trim()).length()>0)       conf.set(TableInputFormat.SCAN_ROW_STOP, endKey);
		
		if(scan_coloumns!=null && scan_coloumns.length>0){
			StringBuilder sb = new StringBuilder();
			for(String col:scan_coloumns){
				sb.append(sb.length()>0?" ":"");
				sb.append("cf:"+col);
			}
			conf.set(TableInputFormat.SCAN_COLUMNS,sb.toString());
		}else{
			//这里注意一下，同上面不能同时出现，否则上面无意义
			conf.set(TableInputFormat.SCAN_COLUMN_FAMILY,"cf");
		}
		
		conf.set(TableInputFormat.SCAN_CACHEBLOCKS,"false");
		conf.set(TableInputFormat.SCAN_BATCHSIZE,"20000");
		
		
		return conf;
	}
	
	
	private static int[] splitArray = new int[]{0,1,2,3,4,5,6,7,8,9};
	
	//月分区
	private static SimpleDateFormat yyMM = new SimpleDateFormat("yyMM");
	private static SimpleDateFormat dd   = new SimpleDateFormat("dd");
	
	//年分区
	private static SimpleDateFormat yy   = new SimpleDateFormat("yy");
	private static SimpleDateFormat mmdd = new SimpleDateFormat("MMdd");
	
	
	private static final  String month  = "month";
	private static final  String year   = "year";
	
	//档案表的分区方式
	private static final  String doc    = "doc";
	
	private static Map<String, String> partitionTypes = new HashMap<String, String>(){
		{
			put("T_SCRIPT_HIS",   month);
			put("E_MP_DAY_READ",  month);
			put("R_COULOMETRY",   year);
			put("W_COLL_OBJ_RELA",doc);
		}
	};
	
	private static Map<String, String[]> columns = new HashMap<String, String[]>(){
		{
			put("T_SCRIPT_HIS",   new String[]{"info"});
			put("W_COLL_OBJ_RELA",new String[]{"MP_ATTR_CODE","PUB_PRIV_FLAG","SUM_ENERGY_FLAG","CT_RATIO","PT_RATIO","TG_ID","LINE_ID"});
			put("E_MP_DAY_READ",  null);//所有列都要计算
			put("R_COULOMETRY",   null);
		}
	};
	
	
	public static Source<ImmutableBytesWritable,Result>[] getEpmisPartitionSources(String tableName,Date date){
		return getEpmisPartitionSources(tableName,date,splitArray,null);
	}
	
	public static Source<ImmutableBytesWritable,Result>[] getEpmisPartitionSourcesForTest(String tableName,Date date){
		return getEpmisPartitionSources(tableName,date,new int[]{0},null);
	}
	
	public static Source<ImmutableBytesWritable,Result>[] getEpmisPartitionSources(String tableName,Date date,String[] _columns){
		return getEpmisPartitionSources(tableName,date,splitArray,_columns);
	}
	
	public static Source<ImmutableBytesWritable,Result>[] getEpmisPartitionSourcesForTest(String tableName,Date date,String[] _columns){
		return getEpmisPartitionSources(tableName,date,new int[]{0},_columns);
	}
	
	/**
	 * 得到某个表，某一天的所有分区的数据(这么说不确切，应该是取余后的间隔)
	 * 1605_3_01 -> 1605_3_01|
	 * ...
	 * 1605_3_31 -> 1605_3_31|----避免算每个月最后一天和下个月第一天
	 */
	private static Source<ImmutableBytesWritable,Result>[] getEpmisPartitionSources(String tableName,Date date,int[] splitArray,String[] _columns){
		List<Source<ImmutableBytesWritable,Result>> sources = new LinkedList<Source<ImmutableBytesWritable,Result>>();
		
		String  partitionType = partitionTypes.get(tableName);
		if(partitionType.equals(month)){
			for(int mod:splitArray){
				String startKey = yyMM.format(date)+"_"+mod+"_"+dd.format(date);
				//不用计算下一天的字符表达，这样足够了
				String endKey = startKey+"|";
				Configuration conf  = getHBaseConfiguration(tableName, startKey, endKey, _columns==null?columns.get(tableName):_columns);
				//需要注意多版本的用法
				Source<ImmutableBytesWritable, Result> source = tableName.equals("T_SCRIPT_HIS")?Source.hBase(conf,true):Source.hBase(conf);
				sources.add(source);
			}
			
			return sources.toArray(new Source[sources.size()]);
		}else if(partitionType.equals(year)){
			for(int mod:splitArray){
				String startKey = yy.format(date)+"_"+mod+"_"+mmdd.format(date);
				String endKey   = startKey+"|";
				Configuration conf  = getHBaseConfiguration(tableName, startKey, endKey, _columns==null?columns.get(tableName):_columns);
				Source<ImmutableBytesWritable, Result> source = Source.hBase(conf);
				sources.add(source);
			}
			
			return sources.toArray(new Source[sources.size()]);
		}else if(partitionType.equals(doc)){
			//档案表的第一个分区即 无开始键-->0 是没有用的，注意一下
			for(int mod:splitArray){
				String startKey = String.valueOf(mod);
				String endKey   = startKey+"|";
				Configuration conf  = getHBaseConfiguration(tableName, startKey, endKey, _columns==null?columns.get(tableName):_columns);
				Source<ImmutableBytesWritable, Result> source = Source.hBase(conf);
				sources.add(source);
			}
			
			return sources.toArray(new Source[sources.size()]);
		}
		
		
		return null;
	}
	
	
	public static Source<ImmutableBytesWritable,Result>[] getSimplePartitionSources(String tableName,String prefix,Date date){
		List<Source<ImmutableBytesWritable,Result>> sources = new LinkedList<Source<ImmutableBytesWritable,Result>>();
		for(int mod:splitArray){
			//prefix_mod_150501_collObjId
			String startKey = prefix+"_"+mod+"_"+dateFm.format(date);
			String endKey   = startKey+"|";
			Configuration conf  = getHBaseConfiguration(tableName, startKey, endKey, columns.get(tableName));
			Source<ImmutableBytesWritable, Result> source = Source.hBase(conf);
			sources.add(source);
		}
		
		return sources.toArray(new Source[sources.size()]);
	}
	
	static final SimpleDateFormat dateFm = new SimpleDateFormat("yyMMdd");
	public static String getYestoday(String today) throws Exception{
		return dateFm.format(new java.sql.Date(dateFm.parse(today).getTime()-24*60*60*1000));
	}
	
}














