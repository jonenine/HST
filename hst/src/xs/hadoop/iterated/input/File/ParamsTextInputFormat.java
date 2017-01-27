package xs.hadoop.iterated.input.File;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import xs.hadoop.Json;

/**
 * 不同的path相同的InputFormat 会将path用","join后传到textInputformat.Input_dir中,所以
 * 自定义的inputformat需要继承TextInputFormat,这样super.getSplits返回的FileSplit会带着实际的path
 * 在非文件形式的source中(比如hbase的),实际的path只是作为一个key,来取得实际的配置
 */
public class ParamsTextInputFormat extends TextInputFormat {

	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException {
		List<InputSplit> inputSplits = super.getSplits(arg0);
		
		//取得分配给这个source的参数
		Json jsonParam = Json.desierialize(arg0.getConfiguration().get("xs.hadoop.piece.iterated.jobParameters"));
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
					
					//使用ParamsFileSplit来传递josn参数
					ParamsFileSplit jfs = ParamsFileSplit.copyToParamsFileSplit(fis);
					jfs.setJsonStr(splitJsonParam.toString());
					
					_inputSplits.add(jfs);
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
	
}
