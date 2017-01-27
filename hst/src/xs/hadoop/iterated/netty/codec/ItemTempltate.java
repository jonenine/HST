package xs.hadoop.iterated.netty.codec;


/**
 * 项的配置
 */
public abstract class ItemTempltate{
	
	public String name;    
	
	public ItemTempltate(String name) {
		this.name = name;
	}
	
	public ItemTempltate(){}
	
	/**
	 * 在解析额的时候,属于这个项的第一个字节到达以前,需要首先直到这个项的长度 
	 * @throws Exception
	 */
	public abstract int length(Parser parser);
	
	/**
	 * 这个项应该从byte[]翻译成什么,如果不定义会以byte[]的方式返回 
	 */
	public abstract Object out(Parser parser) ;
	
	/**
	 * 在返回byte的数量达到length()的时候,会调用这个方法
	 */
	public abstract boolean validate(Parser parser);
	
}




