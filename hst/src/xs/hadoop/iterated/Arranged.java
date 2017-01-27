package xs.hadoop.iterated;

import java.util.HashMap;
import java.util.Map;

import xs.hadoop.Json;


/**
 *  作为job和scheduler的基础接口,形成任务之间关系，其中包括任务和子任务的关系
 */
public abstract class Arranged {
	
	protected String name;
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getName(){
		return this.name;
	}
	
	/**
	 * 在制定路径上执行当前方法
	 * 这个方法必须是同步方法 
	 * @param path 制定当前任务的输出路径
	 */
	public abstract void run(String path) throws Exception;
	
	/**
	 * 得到最近一个任务的输出路径 
	 */
	public abstract String getLastRunPath();
	
	
	
	protected Map<String, Json> result = new HashMap<String, Json>();
	
	/**
	 * 向父任务报告成果,当一个child完成之后,由父任务负责调用，完成汇报。
	 */
	public void reportResultToParent() throws Exception{
		//如果一个调度是串行的,最后只有一个结果,那么这个调度的名称也会绑定这唯一的结果,对于最底层的iJob而言,没什么变化
		if(result.size()==1) result.put(getName(), result.values().iterator().next());
		
		Arranged parent = getParent();
		if(parent!=null){
			for(String key:result.keySet()){
				synchronized (parent.result) {
					//任务及调度名称都不允许重复
					if(parent.result.containsKey(key)) throw new Exception("Arranged name deplicated error");
					parent.result.put(key, result.get(key));
				}
			}
		}
	}
	
	protected Arranged parnet;
	
	public void setParent(Arranged parnet){
		this.parnet = parnet;
	}
	
	public Arranged getParent(){
		return this.parnet;
	}
	
	/**
	 * 得到根部的 Scheduler
	 */
	public Scheduler getScheduler(){
		Arranged the = this;
		Arranged _parent;
		while((_parent = the.parnet)!=null) the = _parent;
		
		return (Scheduler) the;
	}
	
	/**
	 * 层层向上寻找以前的执行结果
	 * @param jobOrArrangedName
	 * @return
	 */
	public Json getPreviousResult(String jobOrArrangedName){
		Arranged parnet = this;
		do{
			if(parnet.result.containsKey(jobOrArrangedName)) return parnet.result.get(jobOrArrangedName);
		}while((parnet = parnet.getParent())!=null);
		
		return null;
	}
	
	
	
	
}





