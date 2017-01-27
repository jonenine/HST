package xs.hadoop.iterated.input.HBase;

import java.util.List;
import java.util.Map;

import xs.hadoop.iterated.input.Transformer;

/* 参考:scan.setMaxVersions(); //指定最大的版本个数。如果不带任何参数调用setMaxVersions，表示取所有的版本。如果不掉用setMaxVersions，只会取到最新的版本.
 * 也就是默认情况
 */
public abstract class HBaseTransformerForAllVersion<K1> extends Transformer<String, Map<String, List<HBaseCell>>,K1> {

	
}
