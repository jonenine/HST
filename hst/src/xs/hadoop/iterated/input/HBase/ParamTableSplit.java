package xs.hadoop.iterated.input.HBase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import xs.hadoop.Json;
import xs.hadoop.iterated.JsonParams;

public class ParamTableSplit extends TableSplit implements JsonParams{
	
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
		fileSplitFields = TableSplit.class.getDeclaredFields();
		List<Field> temp = new LinkedList<Field>();
		for(Field f:fileSplitFields){
			if(!Modifier.isStatic(f.getModifiers())){
				f.setAccessible(true);
				temp.add(f);
			}
		}
		fileSplitFields = temp.toArray(new Field[temp.size()]);
	}
	
	/**
	 * 创建新对象,范型拷贝属性并返回
	 */
	public static ParamTableSplit copyToParamsFileSplit(TableSplit fileSplit) throws Exception{
		ParamTableSplit jsonParamsSplit = new ParamTableSplit();
		for(Field f:fileSplitFields){
			f.set(jsonParamsSplit, f.get(fileSplit)) ;
		}
		
		return jsonParamsSplit;
	}
}
