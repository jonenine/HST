package xs.hadoop.iterated.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import xs.hadoop.Json;
import xs.hadoop.iterated.JsonParams;

/**
 * 作为非文件类型的通用inputsplit,直接使用,不用继承 
 */
public class ParamsInputSplit extends InputSplit implements Writable,JsonParams {
	
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
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(getJsonStr());
	}

	public void readFields(DataInput in) throws IOException {
		setJsonStr(in.readUTF());
	}

	public long getLength() throws IOException, InterruptedException {
		return 0;
	}

	public String[] getLocations() throws IOException, InterruptedException {
		return null;
	}


}
