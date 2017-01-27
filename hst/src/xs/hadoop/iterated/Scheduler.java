package xs.hadoop.iterated;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import xs.hadoop.Json;
import xs.hadoop.iterated.netty.JsonListenFactory.JsonListen;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

/**
 * 调度器
 */
public class Scheduler extends SerialArrangment implements JsonParams{
	
	private String scheduleName;
	
	
	public String getName(){
		return this.scheduleName;
	}
	
	
	public Scheduler(String scheduleName,Configuration configuration,String[] args) throws IOException{
		super(configuration);
		this.scheduleName = scheduleName;
		init(args);
	}
	
	
	public Scheduler(String scheduleName,String[] args) throws IOException{
		this(scheduleName,new Configuration(),args);
	}
	
	
	String[] toolArgs;
	
	private void init( String[] args) throws IOException {
		GenericOptionsParser parser = new GenericOptionsParser(this.conf, args);
		toolArgs = parser.getRemainingArgs();
		if(toolArgs != null && toolArgs.length>0){
			if(jsonParams == null) jsonParams = Json.createJson();
			for(int i=0;i<toolArgs.length;i++){
				String arg = toolArgs[i];
				jsonParams.addOrUpadte(".", "arg"+i, new Json(arg));
			}
		}
	}
	
	
	Json jsonParams;

	public void setJsonStr(String jsonStr) {
		this.jsonParams = Json.desierialize(jsonStr);
	}

	public String getJsonStr() {
		return jsonParams.toString();
	}
	
	public Json getJson() {
		return jsonParams;
	}
	
	/**
	 * 得到通过main方法传进来的参数
	 * @param pIndex
	 * @return
	 */
	public String getDriverParam(int pIndex){
		if(jsonParams!=null){
			Json param = jsonParams.find("arg"+pIndex);
			return param == null?null:param.toString();
		}
		return null;
	}
	
	
	boolean registed = false;
	public synchronized void run(String path) throws Exception{
		if(!registed){
			IteratedUtil.getMessageManager().regist("/"+scheduleName,new JsonListen(){
				public void put(String clientIp, int messageId, Json json) throws InterruptedException {
					Json error = json.find("error");
					if(error!=null){
						System.err.println(clientIp+":"+error.toString());
					}
					
					Json message = json.find("message");
					if(message!=null){
						String msg = message.toString();
						String[] msgs = msg.split(":");
						//结构化消息
						if(msgs.length>1){
							if(msgs[0].equals("counter")) putCounterMessage(clientIp,msgs);
						}
						//纯消息
						else{
							System.out.println(clientIp+":"+msg);
						}
					}
				}
			});
			
			showCounterThread.start();
			registed = true;
		}//!if(!registed)
		
		super.run(path);
		
		showCounterThread.interrupt();
		showCounterThread.join(10000);
		runShowCounterOnce();
		
		IteratedUtil.getMessageManager().unRegist("/"+scheduleName);
	}
	
	//<counterName,clientIp,[]>
	Table<String, String, long[]> counterMessahes = HashBasedTable.create();
	void putCounterMessage(String clientIp,String[] msgs){
		String name    = msgs[1];
		String sum     = msgs[2];
		//先按照name，再按照ip
		//[now,last]
		long[] sums = counterMessahes.get(name, clientIp);
		if(sums==null){
			sums = new long[2];
			counterMessahes.put(name, clientIp, sums);
		}
		
		sums[0] = Long.parseLong(sum);
		
	}
	
	
	/**
	 * 在所有的任务结束后，显示counter的最终值 
	 */
	private void runShowCounterOnce() throws Exception{
		//<counterName,<ip,Long[]>>
		Map<String, Map<String, long[]>> columnMap = counterMessahes.rowMap();
		
		//<counterName,Long>
		Map<String, Long> sumsForCount = new HashMap<String, Long>();
		//<counterName,Long>
		Map<String, Long> intervalForCount = new HashMap<String, Long>();
		
		//按counter名称迭代
		for(String counterName:columnMap.keySet()){
			//<ip,counter>
			Map<String, long[]> countersSameName = columnMap.get(counterName);
			
			//<clientIp,StringBuilder>
			Map<String, StringBuilder> messageMap = new HashMap<String, StringBuilder>();
			
			//在每个counter下面按照client ip迭代
			for(String clientIp:countersSameName.keySet()){
				long[] sums  = countersSameName.get(clientIp);
				long inteval = sums[0]-sums[1];
				sums[1] = sums[0];
				
				//这个countName下面的总的
				Long sumAll = sumsForCount.get(counterName);
				sumsForCount.put(counterName,(sumAll==null?0:sumAll)+sums[0]);
				Long intervalAll = intervalForCount.get(counterName);
				intervalForCount.put(counterName, (intervalAll==null?0:intervalAll)+inteval);
				
				//统计这个counterName下面每个ip的
				StringBuilder sb = messageMap.get(clientIp);
				if(sb == null) {
					sb = new StringBuilder();
					messageMap.put(clientIp, sb);
				} 
				sb.append(clientIp+":"+sums[0]+"("+inteval+")");
			}
			
			//显示
			StringBuilder sb = new StringBuilder();
			for(String clientIp:countersSameName.keySet()){
				sb.append((sb.length()==0?"":"\t")+messageMap.get(clientIp));
			}
			
			String message = "\t计数器信息:"+counterName+",所有节点:"+sumsForCount.get(counterName)+"("+intervalForCount.get(counterName)+")"
					+"\t,分ip情况---"+sb.toString();
			
			System.out.println(message);
			
			Thread.sleep(10);
		}
	}
	
	Thread showCounterThread = new Thread(new Runnable() {
		public void run() {
			try {
				while(true){
					runShowCounterOnce();
					Thread.sleep(1000);
				}
			} catch (Exception e) {}
		}//~run
	});
	

	public void run() throws Exception{
		run(null);
	}
	
	
	
	
}


