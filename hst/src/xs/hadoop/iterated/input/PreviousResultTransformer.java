package xs.hadoop.iterated.input;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import xs.hadoop.Json;
import xs.hadoop.iterated.Collector;


/**
 * 在一次调度中存在多次迭代的任务，当引用上一个任务的结果时使用的Transformer
 */
public abstract class PreviousResultTransformer<K1> extends Transformer<Long, String, K1> {
	
	protected Map<String,Integer> lastOutputfieldNamesOrder;
	
	String[] nullArray;
	
	public void setup(Json jsonParam) throws Exception {
		super.setup(jsonParam);
		//从source直接设置过来,这一点很重要
		Json _order = this.jsonParam.find("lastOutputfieldNamesOrder");
		if(_order!=null){
			List<Json> list = _order.asList();
			lastOutputfieldNamesOrder = new HashMap<String, Integer>();
			int i = 0 ;
			for(Json field:list){
				lastOutputfieldNamesOrder.put(field.toString(), i++);
			}
		}
		
		nullArray = new String[lastOutputfieldNamesOrder.keySet().size()];
	}

	
	protected String getFiledKey(String[] inputValues){
		return inputValues[0];
	}
	
	protected String getFiledValue(String[] inputValues,String fieldName){
		if(lastOutputfieldNamesOrder!=null){
			return inputValues[lastOutputfieldNamesOrder.get(fieldName)+1];
		}
		
		return null;
	}

	public void flatMap(Long inputKey, String inputValue,Collector<K1> collector) throws Exception {
		if(inputValue!=null && inputValue.length()!=0){
			//别忘了去掉\t?????
			String[] _values = inputValue.split("\\t");
			flatMap(inputKey,_values,collector);
		}else{
			//啥没有
			flatMap(inputKey,nullArray,collector);
		}
	}

	abstract public void flatMap(Long inputKey, String[] inputValues,Collector<K1> collector);

}



