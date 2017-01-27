package xs.hadoop.iterated.input.HBase;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import xs.hadoop.Json;
import xs.hadoop.iterated.JsonParams;

/**
 * 
 */
public class ParamsHBaseInputFormat extends TextInputFormat {

	/**
	 * 将父类接口中的范型信息去掉
	 */
	public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) {
		//无论如何,split肯定是JsonParams子类,这里得到每个split内部的配置
		Json splitJsonParam = ((JsonParams)split).getJson();
		try {
			//创建tableinputformat
			TableInputFormat tableInputFormat = createTableInputFormat(splitJsonParam);
			//生成实际的recordreader,context参数没用
			return tableInputFormat.createRecordReader(split, context);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	
	public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
		List<InputSplit> inputSplits = super.getSplits(jobContext);
		
		//取得分配给这个source的参数
		Json jsonParam = Json.desierialize(jobContext.getConfiguration().get("xs.hadoop.piece.iterated.jobParameters"));
		
		List _inputSplits = new LinkedList();
		for(InputSplit inputSplit:inputSplits){
			if(inputSplit instanceof FileSplit){
				try {
					//取得实际的path
					FileSplit fis = (FileSplit)inputSplit;
					String path = fis.getPath().toString();
					//将path添加到实际的InputSplit的参数中,供DelegatingMapperHack使用,并最终实际注入到InjectionMapper中
					String params = jsonParam.find(path.replace(".", "//")).toString();
					Json splitJsonParam = Json.desierialize(params);
					splitJsonParam.addOrUpadte(".", "path", new Json(path));
					
					//创建TableInputFormat
					TableInputFormat tableInputFormat = createTableInputFormat(splitJsonParam);
					
					//getSplits
					List<InputSplit> tableInputSplits = tableInputFormat.getSplits(jobContext);
					
					//将splits转换成jsonParams
					for(InputSplit is:tableInputSplits){
						ParamTableSplit ptis = ParamTableSplit.copyToParamsFileSplit((TableSplit) is);
						//不要忘记,只有如此配置项才能从driver被带到运行mapper的节点
						ptis.setJsonStr(splitJsonParam.toString());
						_inputSplits.add(ptis);
					}
					
				} catch (Exception e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}else{
				throw new RuntimeException("inputSplit:"+inputSplit.getClass().getName()+" is not belong to FileSplit class");
			}
		}
		
		return _inputSplits;
	}

	
	/**
	 * 创建TableInputFormat并初始化
	 * @throws IOException 
	 */
	private static TableInputFormat createTableInputFormat(Json splitJsonParam) throws IOException{
		Configuration conf = copyToConfiguration(splitJsonParam.find("sourceInitParams"));
		
		HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));
		Job jobCopy = Job.getInstance(conf);
		//TableMapReduceUtil.initCredentials(jobCopy);
		
		conf = jobCopy.getConfiguration();
		
		TableInputFormat tableInputFormat = ReflectionUtils.newInstance(TableInputFormat.class, conf);
		return tableInputFormat;
	}
	
	
	public static final String[] HBaseConstants = {
		"hbase.zookeeper.property.clientPort",
		"zookeeper.znode.parent",
		"hbase.zookeeper.quorum",
		TableInputFormat.INPUT_TABLE,
		TableInputFormat.SCAN,
		TableInputFormat.SCAN_ROW_START,
		TableInputFormat.SCAN_ROW_STOP,
		TableInputFormat.SCAN_COLUMNS,
		TableInputFormat.SCAN_COLUMN_FAMILY,
		TableInputFormat.SCAN_TIMESTAMP,
		TableInputFormat.SCAN_TIMERANGE_START,
		TableInputFormat.SCAN_TIMERANGE_END,
		TableInputFormat.SCAN_MAXVERSIONS,
		TableInputFormat.SCAN_CACHEDROWS,
		TableInputFormat.SCAN_BATCHSIZE,
		TableInputFormat.SCAN_CACHEBLOCKS
	} ;
	
	public static Json copyToJson(Configuration conf){
		Json json = Json.createJson();
		for(String confFieldName:HBaseConstants){
			String str= conf.get(confFieldName);
			if(str!=null){
				json.addOrUpadte(".", confFieldName.replace(".", "//"), new Json(str));
			}
		}
		return json;
	}
	
	/**
	 * 通过json配置项,生成Configuration
	 */
	private static Configuration copyToConfiguration(Json splitJsonParam){
		Configuration conf = new Configuration();
		for(String confFieldName:HBaseConstants){
			Json json = splitJsonParam.find(confFieldName.replace(".", "//"));
			
			if(json!=null){
				conf.set(confFieldName, json.toString());
			}
		}
		return conf;
	}
}



