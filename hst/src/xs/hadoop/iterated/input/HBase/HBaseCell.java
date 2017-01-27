package xs.hadoop.iterated.input.HBase;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseCell{
	
	String value;
	Long timeStamp;
	public String getValue() {
		return value;
	}
	public Long getTimeStamp() {
		return timeStamp;
	}
	public HBaseCell(String value, Long timeStamp) {
		this.value = value;
		this.timeStamp = timeStamp;
	}
	
	/* 参考:scan.setMaxVersions(); //指定最大的版本个数。如果不带任何参数调用setMaxVersions，表示取所有的版本。如果不掉用setMaxVersions，只会取到最新的版本.
	 * 也就是默认情况
	 *
	 */
	public static Map<String, String> getRowForLastVersion(Result result){
       Map<String, String> values = new HashMap();
       if (result != null) {
           for(Cell cell:result.rawCells()){
           	String value     = Bytes.toString(CellUtil.cloneValue(cell));
           	String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
           	
           	values.put(qualifier, value);
           }
       }
       return values;
	}
	
	public static Map<String, List<HBaseCell>> getRowForAllVersions(Result result){
		Map<String, List<HBaseCell>> re = new HashMap<String, List<HBaseCell>>();
       
       if (result != null) {
           for(Cell cell:result.rawCells()){
           	String value     = Bytes.toString(CellUtil.cloneValue(cell));
           	long timeStamp   = cell.getTimestamp();
           	
           	String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
           	List<HBaseCell>  hBaseCells= re.get(qualifier);
           	if(hBaseCells==null){
           		hBaseCells = new LinkedList<HBaseCell>();
           		re.put(qualifier, hBaseCells);
           	}
           	
           	hBaseCells.add(new HBaseCell(value, timeStamp));
           }
       }
       return re;
	}
}



