package xs.hadoop;
import java.text.ParseException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {
	/**
	 * 正则表达式对象
	 * @author Administrator
	 *
	 */
	public static class RegXp{
		Pattern pattern;
		String target;
		Boolean isMatch;
		List<List<String>> subMatchStr=new LinkedList<List<String>>();
		private RegXp(String patternStr, String target) {
			//
			pattern=Pattern.compile(patternStr);
			this.target=target;
			//
			isMatch= pattern.matcher(target).matches();
			//
			Matcher macher = pattern.matcher(target);
			while(macher.find()){
				List<String >temp=new LinkedList<String>();
				for(int i=1;i<macher.groupCount()+1;i++){
					String curGroup;
					if((curGroup=macher.group(i))!=null){
						temp.add(curGroup);
					}
				}
				subMatchStr.add(temp);
			}
		}
		/**
		 * 得到是否返回
		 * @return
		 */
		public boolean isMatch(){
			return isMatch;
		}
		
		public int subMatchSize(){
			return subMatchStr.size();
		}
		
		/**
		 * 得到子匹配
		 * @return
		 */
		public List<String> getSubMatchs(int index){
			if(index>subMatchStr.size()-1){
				return null;
			}
			return subMatchStr.get(index);
		}
	}
	/**
	 * 得到正则表达式对象
	 * @param patternStr
	 * @param target
	 * @return
	 */
	public static RegXp getRegXp(String patternStr, String target){
		return new RegXp(patternStr,target);
	}
	
	//
	/**
	 * @param target      目标字符串
	 * @param index       替换位置
	 * @param replaceStr  替换字符串
	 */
	public static String replace(String target,int index,String replaceStr){
		int length=replaceStr.length();
		return target.subSequence(0, index)+replaceStr+target.subSequence(index+length, target.length());
	}
	
	/**
	 * @param target      目标字符串
	 * @param index       替换位置
	 * @param replaceStr  替换字符串
	 */
	public static String insert(String target,int index,String replaceStr){
		return target.subSequence(0, index)+replaceStr+target.subSequence(index,target.length());
	}
	
	/**
	 * 取得一个正则表达式匹配的字符串在一个字符串中的位置
	 * @param target
	 * @param patternStr   不要加(),程序会自动加
	 * @return Object[] 第一个元素为匹配的字符串,而2,3为匹配的起终位置
	 */
	public static Object[] positionOf(String target,String patternStr){
		RegXp reg = StringUtils.getRegXp("("+patternStr+")", target);
		String str=null;
		if(reg.getSubMatchs(0)!=null  && reg.getSubMatchs(0).size()>0){
			str=reg.getSubMatchs(0).get(0);
			int start=target.indexOf(str);
			int end=start+str.length();
			return new Object[]{str,start,end};
		}
		return null;
	}
	
	/**
	 * 
	 * @param target
	 * @param patternStr
	 * @return Object[] 第一个元素为匹配的字符串,而2,3为匹配的起终位置
	 */
	public static List<Object[]> positionsOf(String target,String patternStr){
		List<Object[]> re=new LinkedList<Object[]>();
		Object[] result;;
		while((result=positionOf(target,patternStr))!=null){
			re.add(result);
			String replaceStr=result[0].toString();
			//replaceFirst第一个参数本来就是正则表达式
			target=target.replaceFirst(replaceStr, getBlankString(replaceStr.length()));
		}
		if(re.isEmpty()) re=null;
		return re;
	}
	
	/**
	 * 生成一个指定长度的空白字符串
	 * @param size
	 * @return
	 */
	public static String getBlankString(int size){
		StringBuffer buf=new StringBuffer();
		for(int i=0;i<size;i++){
			buf.append(" ");
		}
		return buf.toString();
	}
	
	public static java.util.Date adaptDate(String dateStr) throws ParseException{
		if(dateStr==null) return null;
		dateStr=dateStr.trim();
		String[] datainfo=dateStr.split("-|\\s|:|,");
		int length=datainfo.length;
		Calendar cal=new GregorianCalendar();
		if(length>=2){
			cal.set(Calendar.YEAR, Integer.parseInt(datainfo[0]));
			cal.set(Calendar.MONTH, Integer.parseInt(datainfo[1])-1);
			cal.set(Calendar.DAY_OF_MONTH, 1);
		}else{return null;}
		if(length>=3){
			
			cal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(datainfo[2]));
		}
		int hour=0;
		if(length>=4){
			hour=Integer.parseInt(datainfo[3]);
		}
		cal.set(Calendar.HOUR_OF_DAY, hour);
		int minute=0;
		if(length>=5){
			minute=Integer.parseInt(datainfo[4]);
		}
		cal.set(Calendar.MINUTE, minute);
		int second=0;
		if(length>=6){
			second=Integer.parseInt(datainfo[5]);
		}
		cal.set(Calendar.SECOND, second);
		long milSecondAdded=0;
		if(length>=7){
			milSecondAdded=Integer.parseInt(datainfo[6]); 
		}
			
		return new java.util.Date(cal.getTime().getTime()+milSecondAdded);
	}
	
}
