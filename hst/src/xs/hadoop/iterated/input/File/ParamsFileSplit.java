package xs.hadoop.iterated.input.File;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import xs.hadoop.Json;
import xs.hadoop.iterated.JsonParams;

public class ParamsFileSplit extends FileSplit implements JsonParams{
	
	Json  jsonParams;

	public void setJsonStr(String jsonStr) {
		this.jsonParams = Json.desierialize(jsonStr);
	}

	public String getJsonStr() {
		return jsonParams.toString();
	}
	
	public Json getJson() {
		return jsonParams;
	}
	
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		setJsonStr(in.readUTF());
	}

	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeUTF(getJsonStr());
	}
	
	
	static Field[] fileSplitFields;
	static{
		fileSplitFields = FileSplit.class.getDeclaredFields();
		for(Field f:fileSplitFields){
			f.setAccessible(true);
		}
	}
	
	/**
	 * 创建新对象,拷贝属性并返回
	 */
	public static ParamsFileSplit copyToParamsFileSplit(FileSplit fileSplit) throws Exception{
		ParamsFileSplit jsonParamsFileSplit = new ParamsFileSplit();
		for(Field f:fileSplitFields){
			f.set(jsonParamsFileSplit, f.get(fileSplit)) ;
		}
		
		return jsonParamsFileSplit;
	}

	
	
}





