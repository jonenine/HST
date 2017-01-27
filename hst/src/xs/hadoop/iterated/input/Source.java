package xs.hadoop.iterated.input;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.mapreduce.InputFormat;

import xs.hadoop.Json;
import xs.hadoop.iterated.JsonParams;
import xs.hadoop.iterated.input.File.ParamsTextInputFormat;
import xs.hadoop.iterated.input.HBase.ParamsHBaseInputFormat;


/**
 * 包装hadoop原生inputFormat体系
 */
public class Source<K0,V0> implements JsonParams {
	
	protected Class<? extends InputFormat> inputFormatClass;
	
	public Class<? extends InputFormat> getInputFormatClass() {
		return inputFormatClass;
	}

	public Source(Class<? extends InputFormat> inputFormatClass) {
		this.inputFormatClass = inputFormatClass;
	}


	protected Json jsonParams;

	public void setJsonStr(String jsonStr) {
		this.jsonParams = Json.desierialize(jsonStr);
	}

	public String getJsonStr() {
		return jsonParams.toString();
	}
	
	public Json getJson() {
		return jsonParams;
	}
	
	/**
	 * 是否同上一个source的order保持一致
	 */
	protected boolean orderAsPrevious = false;

	public boolean isOrderAsPrevious() {
		return orderAsPrevious;
	}

	public void setOrderAsPrevious(boolean orderAsPrevious) {
		this.orderAsPrevious = orderAsPrevious;
	}
	
	
	protected Map<String, String> params = new HashMap<String, String>();
	
	public Source setParam(String name,String value){
		Json sourceParams = jsonParams.find("sourceParams");
		if(sourceParams == null) {
			sourceParams = Json.createJson();
		}
		sourceParams.addOrUpadte(".", name, new Json(value));
		
		//必须设置回去才能生效,可以加一个replace函数传一个回调参数,自动做
		jsonParams.addOrUpadte(".", "sourceParams", sourceParams);
		
		return this;
	}

	/**
	 * 创建基于file system,比如hdfs的input format 
	 * 对于这类而言,inputPath的url足能说明问题
	 */
	public static Source<Long,String> textFile(String inputPath){
		return textFile(inputPath,null);
	}
	
	public static Source<Long,String> textFile(String inputPath,Map<String, String> _params){
		Source source = new Source(ParamsTextInputFormat.class);
		source.params = _params;
		//设置输入路经
		Json json = Json.createJson();
		json.addOrUpadte(".", "filePath", new Json(inputPath));
		if(source.params!=null) json.addOrUpadte(".", "sourceParams", new Json(source.params));
		source.setJsonStr(json.toString());
		
		return source;
	}
	
	/**
	 * 同spark hadoop rdd的构建方式类似 
	 */
	public static Source<ImmutableBytesWritable,Result> hBase(Configuration conf,boolean isMultiVersion){
		return hBase(conf,null,isMultiVersion);
	}
	
	public static Source<ImmutableBytesWritable,Result> hBase(Configuration conf){
		return hBase(conf,false);
	}
	
	public static Source<ImmutableBytesWritable,Result> hBase(Configuration conf,Map<String, String> _params,boolean isMultiVersion){
		Source source = new Source(ParamsHBaseInputFormat.class);
		source.params = _params;
		
		//不加这个，数据会写入regionserve的cache，可能会导致regionserver做gc，导致读写均卡死
		conf.setBoolean(TableInputFormat.SCAN_CACHEBLOCKS, false);
		
		//增加batch size
		conf.set(TableInputFormat.SCAN_CACHEDROWS,  "1000");
		conf.set(TableInputFormat.SCAN_BATCHSIZE,   "1000");
		
		if(isMultiVersion) conf.setInt(TableInputFormat.SCAN_MAXVERSIONS, Integer.MAX_VALUE);
		else               conf.setInt(TableInputFormat.SCAN_MAXVERSIONS, 1);
		
		//将hbase的配置信息序列化到json中去
		Json json = ParamsHBaseInputFormat.copyToJson(conf);
		if(source.params!=null) json.addOrUpadte(".", "sourceParams", new Json(source.params));
		source.setJsonStr(json.toString());
		
		return source;
	}
	
}






