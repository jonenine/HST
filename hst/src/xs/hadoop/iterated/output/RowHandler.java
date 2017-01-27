package xs.hadoop.iterated.output;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import xs.hadoop.iterated.IteratedUtil;
import xs.hadoop.iterated.IteratedUtil.MergeRepeat;

/**
 * 取出join后的数据
 */
public class RowHandler{
	
	List<List<Map<String,Object>>> values;
	
	public RowHandler(List<List<Map<String,Object>>>  values){
		this.values = values;
	}
	
	/**
	 * 得到某一个source的返回值,这是最复杂的情况,就一个key值,有分别来自不同source的多行进行join
	 */
	public List<Map<String,Object>> getRows(int order){
		if(order >= values.size()) return null;
		List<Map<String, Object>> rows = values.get(order);
		if(rows!=null){
			for(Map<String, Object> row:rows){
				if(row!=null) row.remove("_order");
			}
		}
		
		return rows;
	}
	
	/**
	 * 每行只有一个值的情况,仍然是多行 
	 */
	public List<Object> getSingleFieldRows(int order){
		List<Map<String,Object>> rows = getRows(order);
		if(rows == null) return null;
		List<Object> re = new ArrayList<Object>(rows.size());
		for(Map<String,Object> row:rows){
			//同collector对应
			re.add(row.get("_"));
		}
		
		return re;
	}
	
	/**
	 * 将同一个source的返回值中对同一个key的多行合并,适合没有多行和列传行的场景
	 */
	public  Map<String,Object> getRow(int order,MergeRepeat mergeRepeat){
		List<Map<String,Object>> value= getRows(order);
		if(value==null) return null;
		Map<String,Object> row = IteratedUtil.mergeMap(value, mergeRepeat);
		if(row!=null) row.remove("_order");
		return row;
	}
	
	/**
	 * 将同一个source的返回值中对同一个key的多行合并,适合没有多行和列传行的场景
	 * ValueHandler和collector一样,row的概念是不包括key的,这里注意一下,也就是一行由key-->row组成
	 */
	public  Map<String,Object> getRow(int order){
		Map<String,Object> row = getRow(order,null);
		if(row!=null) row.remove("_order");
		return row;
	}
	
}


