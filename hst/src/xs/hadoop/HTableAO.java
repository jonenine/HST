package xs.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.InitializingBean;

/**
 * 类似于DAO，访问hbase的对象
 * 这里实现了的spring的接口，但不一定非要使用spring
 */
public class HTableAO implements InitializingBean{
	
	boolean isWal = true;
	
	public void setWal(boolean isWal) {
		this.isWal = isWal;
	}

	protected String defaultTableName;
	
	int zookeeperClientPort = 2181;
	String zookeeperQuorum;
	
	
	public void setZookeeperClientPort(int zookeeperClientPort) {
		this.zookeeperClientPort = zookeeperClientPort;
	}
	public void setTableName(String tableName) {
		this.defaultTableName = tableName;
	}
	public void setZookeeperQuorum(String zookeeperQuorum) {
		this.zookeeperQuorum = zookeeperQuorum;
	}
	
	
	protected HTable table = null;
	protected Configuration config = null;
    
    public void afterPropertiesSet() throws Exception {
    	  config = HBaseConfiguration.create();
          config.set("hbase.zookeeper.property.clientPort", String.valueOf(zookeeperClientPort));
          config.set("hbase.zookeeper.quorum", zookeeperQuorum);
		  //亿力平台特有
		  config.set("zookeeper.znode.parent", "/hbase-unsecure");
          resetHTable();
	}


    public synchronized void flushHTable() throws IOException{
		try{
			if(table!=null) table.flushCommits();
		}catch(Exception e){
			e.printStackTrace();
		}
    }

	public synchronized void close(){
		if(table!=null){
			try{
				flushHTable();
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				try{
					table.close();
				}catch(Exception e2){}
				table = null;
			}
		}
	}
    
    public synchronized void resetHTable(){
        if(table!=null){
            try{
            	flushHTable();
            }catch(Exception e){
                e.printStackTrace();
            }finally{
                try{
                	table.close();
                }catch(Exception e2){}
                table = null;
            }
        }

        try{
            table = new HTable(config, Bytes.toBytes(defaultTableName));
            table.setAutoFlush(false, false);
            table.setWriteBufferSize(6291456);
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }
    
    static final byte[] cf  = Bytes.toBytes("cf");
    
    static final Map<String, byte[]> fieldMap = new HashMap<String, byte[]>();
    
    static final int dBatchSize = 1000;

	/**
	 * @param key
	 * @param items   每一个元素都是二维数组[fieldName，value]
	 * @return
	 * @throws Exception
	 */
    public  Exception hbaseInsert(String key ,String[][] items) throws Exception{
    	int i = 0;
    	Exception e1 = null;
    	do{
    		try {
				_hbaseInsert(key,items);
			} catch (Exception e) {
				e1 = e;
				resetHTable();
				try {
					Thread.sleep(100);
				} catch (InterruptedException e2) {}
			}
    	}while(e1!=null && i++<3);
    	
    	return e1;
    }
    
    public  Exception hbaseInsert(String key ,Map<String, ?> row) throws Exception{
    	String[][] items = new String[row.size()][2];
    	int i = 0;
    	for(String fieldName:row.keySet()){
    		Object v = row.get(fieldName);
    		String vstr;
    		//watch out!tostring without trim
    		if(v!=null && (vstr=v.toString()).length()>0) items[i++] = new String[]{fieldName,vstr};
    	}
    	return hbaseInsert(key,items);
    }

    private void _hbaseInsert(String key ,String items[][]) throws Exception{
   	 
    	Put put = new Put(Bytes.toBytes(key));
    	
    	for(String[] item:items){
    		//
    		String fieldName   = item[0];
    		byte[] _fieldName  = fieldMap.get(fieldName);
    		if(_fieldName==null){
    			_fieldName  = Bytes.toBytes(fieldName);
    			fieldMap.put(fieldName, _fieldName);
    		}
    		//	
            String value = item[1];
            put.add(cf, _fieldName, Bytes.toBytes(value));
    	}
    	
    	if(!isWal) put.setDurability(Durability.SKIP_WAL);

    	synchronized (this) {
    		table.put(put);
    		//优化
    		//table.put(List);
		}
    }

    
    public void flushMemStore(){
    	HBaseAdmin admin = null;
    	try {
			admin = new HBaseAdmin(config);
			admin.flush(defaultTableName);
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			if(admin!=null)
				try {
					admin.close();
				} catch (Exception e) {}
		}
    }

	/**
	 * 原子的操作一行
	 * @param valueExpected  null为还没有插入这个cell
	 * @param valueUpdate
	 */
	public boolean checkAndPut(String row,
							   String family,
							   String qualifier,
							   String valueExpected,
							   String valueUpdate) throws Exception{
		Put put = new Put(Bytes.toBytes(row));
		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(valueUpdate));

		return table.checkAndPut(Bytes.toBytes(row),
				                 Bytes.toBytes(family),
				                 Bytes.toBytes(qualifier),
				                 valueExpected==null?null:Bytes.toBytes(valueExpected),
				                 put);
	}
	
	public void delete(String hBasekey) throws Exception{
		Delete del = new Delete(Bytes.toBytes(hBasekey));
		table.delete(del);
		table.flushCommits();
	}
	
	
	/**
	 * 对某一个分区进行大合并
	 */
	public void majorCompaction(String tableName ,String keyInRegion)throws Exception{
		HTable table2 = new HTable(config, Bytes.toBytes(tableName));
		HBaseAdmin admin = null;

		try {
			HRegionLocation loc = table2.getRegionLocation(Bytes.toBytes(keyInRegion));
			if(loc!=null){
				HRegionInfo ri  = loc.getRegionInfo();

				admin = new HBaseAdmin(config);
				admin.majorCompact(ri.getRegionName());
				admin.close();
				admin = null;
			}
		} finally {
			if(table2!=null) {
				try {
					table2.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			if(admin!=null) {
				try {
					admin.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

	}

	/**
	 * 删除表中数据，保留分区
	 * @param tableName
	 * @throws Exception
	 */
	public void truncateTable(String tableName) throws Exception{
		HBaseAdmin admin = new HBaseAdmin(config);
		try {
			byte[] tn = Bytes.toBytes(tableName);
			//不知道什么原因，亿力的平台不支持truncateTable，shell是可以的
			List<HRegionInfo> regions = admin.getTableRegions(tn);
			List<byte[]> endKeys = new LinkedList<byte[]>();
			if(regions.size()>1){
				for(HRegionInfo rInfo:regions) endKeys.add(rInfo.getEndKey());
			}

			HTableDescriptor td = admin.getTableDescriptor(tn);
			//
			if(admin.isTableEnabled(tn)) admin.disableTable(tn);
			admin.deleteTable(tn);

			admin.createTable(td);
			admin.close();

			//重新分区可不是个容易做的事情
			if(regions.size()>1){
				System.out.println("table:" + tableName + " region size:" + regions.size());
				for(int i=0;i<endKeys.size();i++) {
					byte[] endKey = endKeys.get(i);
					String _endKey = Bytes.toString(endKey==null?new byte[0]:endKey);

					if(_endKey.length()>0){
						System.out.println("table:" + tableName + " is going to split for key:" + _endKey);
						admin = new HBaseAdmin(config);
						admin.split(tn, endKey);
						admin.flush(tn);
						admin.close();
						//
						promiseAllRegionIsOnline(tableName,_endKey);
					}
				}
			}

		}catch(Exception e){
			e.printStackTrace();
		}finally {
			admin.close();
		}
	}

	/**
	 * 创建表(其他表)，如果表不存在
	 * @param tableName
	 * @param familyName
	 * @return 表不存在返回false，存在返回true
	 * @throws Exception
	 */
	public boolean createTableIfNotExist(String tableName,String familyName,Boolean inMemory) throws Exception{
		//如果表不存在就创建表
		HBaseAdmin admin = new HBaseAdmin(config);
		TableName tn = TableName.valueOf(Bytes.toBytes(tableName));
		boolean isExit = admin.tableExists(tn);
		if(isExit) return true;

		try {
			HTableDescriptor desc = new HTableDescriptor(tn);
			HColumnDescriptor cf  = new HColumnDescriptor(familyName);
			cf.setInMemory(inMemory);
			desc.addFamily(cf);
			admin.createTable(desc);
		} finally{
			admin.close();
		}

		//确认表存在
		admin = new HBaseAdmin(config);
		try {
			int i = 0;
			while(!admin.tableExists(tn) && i++<3){
                Thread.sleep(1000);
            }
			if(!admin.tableExists(tn)) throw new Exception("table "+tableName+" can't be created");
		} finally {
			admin.close();
		}

		return false;
	}


	/**
	 * 所有的region都在线，而且包括新插入的region(根据hbase0.98做了修改)
	 * @param newRegionStartKey
	 * @throws Exception
	 */
	protected void promiseAllRegionIsOnline(String tableName, String newRegionStartKey)throws Exception {
		HTable table2 = null;

		try {
			table2 = new HTable(config, Bytes.toBytes(tableName));

			//1.先判断新分出来的region的请求已经提交并生效了，因为操作是异步的，所以这里要进行判断
			boolean hasNewRegion = false;

			do {
                for (Map.Entry<HRegionInfo, ServerName> loc : table2.getRegionLocations().entrySet()) {
                    byte[] _startKey = loc.getKey().getStartKey();
                    if (Bytes.toString(_startKey).equals(newRegionStartKey)) hasNewRegion = true;
                }
                if (!hasNewRegion) Thread.sleep(1000);
            } while (!hasNewRegion);

			//2.判断所有的region都在线
			boolean allIsOnLine = true;

			do {
                for (Map.Entry<HRegionInfo, ServerName> loc : table2.getRegionLocations().entrySet()) {
                    if (loc.getKey().isOffline()) {
                        allIsOnLine = false;
                        break;
                    }
                }
                if(!allIsOnLine) Thread.sleep(1000);
            } while (!allIsOnLine);
		} finally{
			if(table2!=null) {
				try {
					table2.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

	}


	public long deleteRows(String tableName ,String startKey,String endKey)throws Exception{
		HTable table2 = new HTable(config, Bytes.toBytes(tableName));
		table2.setAutoFlush(false, false);
		table2.setWriteBufferSize(6291456);
		
		long end   = 0;
		try {
			//这样一边查一边删总觉的有问题！
			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes(startKey));
			scan.setStopRow(Bytes.toBytes(endKey));
			//过滤，只保留rowkey
			scan.setFilter(new KeyOnlyFilter());

			ResultScanner rs = table2.getScanner(scan);
			Result r;
			int i = 0;
			List<Delete> batch = new ArrayList<Delete>(dBatchSize);
			long startTime = System.currentTimeMillis();
			long start = 0;
			end = 0;
			while((r = rs.next())!=null){
                Delete del  = new Delete(r.getRow());
				//不向wal中写
				del.setDurability(Durability.SKIP_WAL);

				batch.add(del);
                if(++i == dBatchSize){
                    table2.delete(batch);
                    table2.flushCommits();
                    batch = new ArrayList<Delete>(dBatchSize);
                    i = 0;
                }
                end++;

                //简单的日志功能
                long currentTime = System.currentTimeMillis();
                if(currentTime - startTime>1000){
                    System.out.println("delete "+(end-start)+" rows in this interval");
                    start = end;
                    startTime = currentTime;
                }
            }
			
			table2.delete(batch);
			table2.flushCommits();
			System.out.println("delete "+(end-start)+" rows in this interval");
			
			table2.close();
			table2 = null;

		} finally{

			if(table2!=null) {
				try {
					table2.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}


		return end;
	}
}
