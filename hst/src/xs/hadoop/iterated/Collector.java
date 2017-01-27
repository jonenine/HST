package xs.hadoop.iterated;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *  transformer和joiner的结果收集器
 */
public class Collector<K1>{
	
	public static class Entry<K,V>{
		protected K key;
		protected V value;
		public Entry(K key, V value) {
			this.key = key;
			this.value = value;
		}
		public K getKey() {
			return key;
		}
		
		/**
		 * 注意,这里实际是get row 
		 */
		public V getValue() {
			return value;
		}
		
		public String toString() {
			return (key!=null?key.toString():"")+":"+(value!=null?value.toString():"");
		}
		
	}
	
	String collectType = null;
	
	static final String multiType  = "multiType";
	static final String singleType = "singleType";
	static final String singleValueType = "singleValueType";
	
	List<Entry<K1, Map<String, Object>>> rows = new LinkedList();
	
	
	/*-------------------------多行模式------------------------*/
	
	/**
	 * 返回多行的模式 
	 */
	public void row(K1 rowKey,Map<String, ?> value){
		if(collectType==null) collectType = multiType;
		else if(!collectType.equals(multiType))throw new RuntimeException("can't mix multiType with "+collectType);
		rows.add(new Entry(rowKey,value));
	}
	
	
	public void row(K1 rowKey,Map value,Integer asOrder){
		if(asOrder!=null ) value.put("_order", asOrder);
		row(rowKey,value);
	}
	
	
	/*-------------------------单行模式------------------------*/
	Map<String, Object> singleRow = new HashMap<String, Object>();
	
	K1 rowKey;
	
	/**
	 * 单行模式 
	 */
	public Collector uniqueRow(K1 rowKey){
		return uniqueRow(rowKey,null);
	}
	
	public Collector uniqueRow(K1 rowKey,Integer asOrder){
		this.rowKey = rowKey;
		if(asOrder!=null ) singleRow.put("_order", asOrder);
		return this;
	}
	
	
	/**
	 * 单行模式,在这里设置多值
	 */
	public Collector setField(String fieldName,Object fieldValue){
		if(collectType==null) collectType = singleType;
		else if(!collectType.equals(singleType)) throw new RuntimeException("can't mix singleType with "+collectType);
		
		//空值自动替换???,其他的方法怎么办,凡是参数是map的都要注意
		if(fieldValue==null) fieldValue = " ";
		
		if(rowKey==null) throw new RuntimeException("please set row key first");
		
		singleRow.put(fieldName, fieldValue);
		
		if(rows.size()==0){
			rows.add(new Entry(rowKey, singleRow));
		}
		
		return this;
	}
	
	/*-------------------------单值模式,但可能是多行------------------------*/
	
	/**
	 * 单值模式,但可能是多行
	 */
	public Collector singleValueRow(K1 rowKey,Object value){
		
		return singleValueRow(rowKey,value,null);
	}
	
	public Collector singleValueRow(K1 rowKey,Object value,Integer asOrder){
		if(collectType==null) collectType = singleValueType;
		else if(!collectType.equals(singleValueType)) throw new RuntimeException("can't mix singleValueType with "+collectType);
		
		Map<String, Object> row= new HashMap();
		row.put("_", value);
		
		if(asOrder!=null ) row.put("_order", asOrder);
		
		rows.add(new Entry(rowKey, row));
		
		return this;
	}
	
	
	public List<Entry<K1, Map<String, Object>>> get(){
		//注意返回一个新创建的副本,否则在多线程环境下会引起错误
		return new ArrayList<Entry<K1,Map<String,Object>>>(rows);
	}
	
	
}



