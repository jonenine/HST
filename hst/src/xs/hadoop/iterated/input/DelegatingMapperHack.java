package xs.hadoop.iterated.input;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.DelegatingMapper;

import xs.hadoop.iterated.JsonParams;


/**
 */
public class DelegatingMapperHack<K1, V1, K2, V2> extends DelegatingMapper<K1, V1, K2, V2> {
	
	InjectionMapper mapper;
	
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		
		try {
			//得到实际的inputsplit 参照DelegatingMapper源代码
			InputSplit inputSplit = context.getInputSplit();
			Method method = inputSplit.getClass().getDeclaredMethod("getInputSplit", new Class[0]);
			method.setAccessible(true);
			
			JsonParams inputSplitReal = (JsonParams) method.invoke(inputSplit, null);
			
			//得到mapper,并进行注入
			Field field = DelegatingMapper.class.getDeclaredField("mapper");
			field.setAccessible(true);
			mapper = (InjectionMapper)(field.get(this));
			
			mapper.setJsonStr(inputSplitReal.getJsonStr());
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	  public void run(Context context) 
		      throws IOException, InterruptedException {
	    setup(context);
	    mapper.run((org.apache.hadoop.mapreduce.Mapper.Context) context);
	    cleanup(context);
	  }
	
}



