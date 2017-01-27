package xs.hadoop.iterated;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import xs.hadoop.Json;


/**
 * 串行任务编排
 */
public class SerialArrangment extends Arranged {
	
	protected Configuration conf;
	
	public SerialArrangment(Configuration configuration){
		this.conf = configuration;
	}
	
	/**
	 * 创建输出路经 
	 */
	public String findOutputDir(String parentPath) throws Exception{
		if(parentPath!=null) return IteratedUtil.rightSlash(parentPath,true) +getName();
		
		return IteratedUtil.findUnusedTempDir("iterated/"+getName(), IteratedUtil.getBaseHdfsUri());
	}
	
	List<Arranged> children = new LinkedList<Arranged>();
	
	/**
	 * 注册一个子任务
	 * 将来需要在串行序列中，再加一个参数，即整个调度结束后，是否清除输出结果，默认是清除，除非设置为true
	 */
	public SerialArrangment arrange(Arranged arranged) throws Exception{
		children.add(arranged);
		return this;
	}
	
	boolean registed = false;
	
	String lastRunPath;
	public synchronized void run(String path) throws Exception{
		String basePath = null;
		for(Arranged arranged:children){
			if(basePath == null) basePath = IteratedUtil.rightSlash(this.findOutputDir(path),true);
			String subPath =  basePath+arranged.getName();
			
			//在job执行之前就确定结果,便于job之间的并行
			if(arranged instanceof IteratedJob){
				IteratedJob iJob = (IteratedJob) arranged;
				Json result = Json.createJson();
				result.addOrUpadte(".", "lastOutputPath", new Json(subPath));
				String[] fieldNamesOrder = iJob.getFieldNamesOrder();
				if(fieldNamesOrder!=null) result.addOrUpadte(".", "lastOutputfieldNamesOrder", new Json(iJob.getFieldNamesOrder()));
				this.result.put(iJob.getName(), result);
			}else{
				arranged.reportResultToParent();
			}
			
			arranged.run(subPath);
			lastRunPath = path;
			
			System.out.println("job complete,out put path "+subPath);
		}
		
	}
	
	public String getLastRunPath() {
		return lastRunPath;
	}

	/**
	 * 创建底层任务
	 */
	public IteratedJob createJob(String name){
		IteratedJob iJob = new IteratedJob(this.conf,name);
		//task和driver通信的时候会用到jobName中的Scheduler名称发信息
		iJob.setJobFullName("/"+getScheduler().getName()+"/"+name);
		iJob.setParent(this);
		return iJob;
	}
	
	
}


