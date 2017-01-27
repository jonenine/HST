package xs.hadoop.iterated.netty.codec;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * 实现ParseCallback,实际是按照异步方式处理解析结果
 */
public abstract class Parser implements ParseCallback{
	
	/**
	 * 整个frame的定义
	 */
	private List<ItemTempltate> frameTemplate;
	public Parser(List<ItemTempltate> frameTemplate) {
		this.frameTemplate = frameTemplate;
	}
	

	private int itemIndex = 0;
	
	/**
	 * 解析到哪一项
	 */
	public int getItemIndex() {
		return itemIndex;
	}

	/**
	 * 这一项的名称
	 */
	public String getItemName() {
		return frameTemplate.get(itemIndex).name;
	}
	
	byte[] currentitemArray;
	/**
	 * 这一项对应的生数据,如果没到长度就返回null
	 * 供function使用,一旦使用了就清空
	 */
	public byte[] getCurrentItemArray() {
		return currentitemArray;
	}
	
	/**
	 * 到目前为止解析出来的item
	 */
	public HashMap<String, Object> itemsSoFar = new HashMap<String, Object>();
	public Map<String, Object> getItemsSoFar() {
		return itemsSoFar;
	};
    /**
     * 到目前为止返回的生数据
     * 将包括第一个item的之前的无效字节,需要两个指针
     * 1.指向第二个item的(使用markReaderIndex),以备这个frame失效时从这个位置开始
     * 2.指针指向当前已经解析完成的字节位置,readerIndex,右边的表示还不能被解析的数据
     */
	private  ChannelBuffer arraySoFar =  ChannelBuffers.dynamicBuffer();
	public ChannelBuffer getArraySoFar() {
		return arraySoFar;
	}
	
	
	/**
	 * 如果出现校验和错误效率会比较低
	 * @param datas
	 * @throws Exception
	 */
	public void parse(byte[] datas) throws Exception{
		//先加到sofar上
		arraySoFar.writeBytes(datas);
		//System.out.println();
		//System.out.println("读入新字节,arraySoFar:"+showChannelBuffer(arraySoFar));
		while(true){
			//得到当前的template
			ItemTempltate curTemplate = frameTemplate.get(itemIndex);
			
			//1.判断长度,数据是否足够解析这个item
			int length = curTemplate.length(this);
			if(arraySoFar.readableBytes() < length) break;//while的出口
			//System.out.println("判断长度通过,试图解析第"+itemIndex+"项");
			//第一项未读之前mark
			if(itemIndex==0) arraySoFar.markReaderIndex();
			//if(itemIndex==0) System.out.println("markReaderIndex");
			//---置itemArray
			this.currentitemArray = new byte[length];
			arraySoFar.readBytes(this.currentitemArray);
			//System.out.println("置第"+itemIndex+"项itemArray : "+showBytes(this.itemArray));
			
			//2.验证
			if(!curTemplate.validate(this)) {
				this.currentitemArray = null;
				//在frame header之后才触发onHasSomethingWrong,比如记日记
				//System.out.println("出错啦!!!--->第"+itemIndex+"项验证");
				if(itemIndex>0) fireHasSomethingWrong();
				/**
				 * 从上次出错的item的第一个字节之后的一个字节开始,复制一个新的ArraySoFar
				 */
				//恢复到上一次的
				arraySoFar.resetReaderIndex();
				//读一个字节指到下一个字节
				arraySoFar.readByte();
				//clear
				remainUnreadedAndClear();
				//System.out.println("出错后,清除,第二字节开始,arraySoFar:"+showChannelBuffer(arraySoFar));
				//从头开始
				continue;
			}
			//3.验证通过,输出这个item
			Object out = curTemplate.out(this);
			//System.out.println("第"+itemIndex+"项验证通过,形成输出"+out);
			this.currentitemArray = null;
			//将结果保存至map			
			itemsSoFar.put(getItemName(), out);
			//如果已经解析完最后一个item
			if(itemIndex==frameTemplate.size()-1){
				try {
					//注意itemsSoFar会被清理掉,所以这里要克隆一下
					this.onFrameClosed((Map<String, Object>)(itemsSoFar.clone()));
				} catch (Exception e) {
					e.printStackTrace();
				}
				//clear
				remainUnreadedAndClear();
				//System.out.println("帧完毕,清除,从新开始,arraySoFar:"+showChannelBuffer(arraySoFar));
				continue;
			}else{
				//循环到下一个item
				itemIndex++;
				//System.out.println("循环到下一个");
			}
		}//~while
	}
	
	
	private void fireHasSomethingWrong(){
		//get discard and readable
		byte[] errorByte = new byte[arraySoFar.writerIndex()];
		arraySoFar.getBytes(0, errorByte);
		try {
			this.onHasSomethingWrong(errorByte,getItemName());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 从外部主动的关闭解析,如果有未使用的数据就触发异常响应机制
	 */
	public void stop(){
		if(itemIndex>0) fireHasSomethingWrong();
		//清一下,使这个对象有重用的可能
		arraySoFar.clear();
		itemIndex =0;
		itemsSoFar.clear();
	}
	
	/**
	 * 开放出来,以便从外部调用
	 */
	public void remainUnreadedAndClear(){
		//将当前可读数据复制到一个新的ArraySoFar中去
		ChannelBuffer newArraySoFar=  ChannelBuffers.dynamicBuffer();
		arraySoFar.readBytes(newArraySoFar, arraySoFar.readableBytes());
		this.arraySoFar = newArraySoFar;
		itemIndex =0;
		itemsSoFar.clear();
	}
	
	/*---------调试---------*/
	public static String showBytes(byte[] bytes){
		StringBuffer re = new StringBuffer();
		for(byte bb:bytes){
			re.append(Integer.toHexString(bb&0xff)+" ");
		}
		return re.toString();
	}
	
	public static byte[] transferChannelBuffer(ChannelBuffer buffer){
		byte[] bytes = new byte[buffer.writerIndex()];
		buffer.getBytes(0, bytes);
		return bytes;
	}
	
	public String showChannelBuffer(ChannelBuffer buffer){
		byte[] bytes = new byte[buffer.writerIndex()];
		buffer.getBytes(0, bytes);
		StringBuffer re = new StringBuffer();
		int i=0;
		for(byte bb:bytes){
			re.append(Integer.toHexString(bb&0xff));
			if(i==buffer.readerIndex()) re.append("^");
			re.append(" ");
			i++;
		}
		return re.toString();
		
	}
	
	
}




