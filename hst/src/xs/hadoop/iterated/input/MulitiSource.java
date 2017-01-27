package xs.hadoop.iterated.input;

import org.apache.hadoop.mapreduce.InputFormat;

import xs.hadoop.Json;


/**
 * 经常有一种情况是统一个source,属于同一个order,而且绑定相同的transformer
 * 比如,一个文件夹下面的所有文件 ,一个hbase表的不同分区等
 */
public abstract class MulitiSource<K0, V0> extends Source<K0, V0> {
	
	public MulitiSource(Class<? extends InputFormat> inputFormatClass) {
		super(inputFormatClass);
	}

	protected abstract Json[] createJsonParams() throws Exception;

	public Source<K0, V0>[] getSources() throws Exception{
		Json[] jsonParams = createJsonParams();
		Source<K0, V0>[] sources = new Source[jsonParams.length]; 
		for(int i=0;i<jsonParams.length;i++){
            Source source = new Source<K0, V0>(getInputFormatClass());
            source.setJsonStr(jsonParams[i].toString());
            //第0个之后的order都和第0个一样
            if(i>0) source.setOrderAsPrevious(true);
            
            sources[i] = source;
		}
		
		return sources;
	}
	
}
