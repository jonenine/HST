package xs.hadoop.iterated.input;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import xs.hadoop.Json;

/**
 * 非文件形式输入的基类
 */
@SuppressWarnings("rawtypes")
public abstract class BaseInputFormat extends InputFormat {
	
	/**
	 * 实际上已经形成了一个有广泛适应性的方法 
	 * 返回的应该是ParamsInputSplit的实例集合
	 */
	public abstract List<InputSplit> getSplits(JobContext jobContext,Json parameters) throws IOException,InterruptedException ;

	
	/**
	 * inputsplit会承担更多的信息,包括recordReader的构造信息
	 */
	public List<InputSplit> getSplits(JobContext jobContext) throws IOException,InterruptedException {
		
		String jobParametersStr = jobContext.getConfiguration().get("xs.hadoop.piece.iterated.jobParameters");
		Json jobParameters = Json.desierialize(jobParametersStr);
		
		//取得绑定的path---这个已知是错误的
		String path = jobContext.getConfiguration().get(FileInputFormat.INPUT_DIR);
		
		//以path作为key,得到配置项
		Json parameters =  jobParameters.find(path);
		
		return getSplits(jobContext,parameters);
	}

}



