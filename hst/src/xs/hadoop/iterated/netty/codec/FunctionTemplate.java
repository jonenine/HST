package xs.hadoop.iterated.netty.codec;

/**
 * function都是无状态的,使用时都是单例的
 */
public class FunctionTemplate extends ItemTempltate{
	public int length(Parser parser) {
		return 0;
	}
	public Object out(Parser parser) {
		return null;
	}
	public boolean validate(Parser parser) {
		return true;
	}
}