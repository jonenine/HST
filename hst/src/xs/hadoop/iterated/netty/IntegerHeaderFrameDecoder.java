package xs.hadoop.iterated.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

public class IntegerHeaderFrameDecoder extends FrameDecoder {
	
	/**
	 * Offset:  0       4                   (Length + 8) 8
	 *         +--------+------------------------+--------
	 *Fields:  | Length | Actual message content | 校验位  |
	  *        +--------+------------------------+--------
	 */
	protected Object decode(ChannelHandlerContext ctx, Channel channel,ChannelBuffer buf) throws Exception {
	     if (buf.readableBytes() < 12) {
	        return null;
	     }

	     //标记可读的最后位置,buf只能读到这个位置及之前的位置
	     buf.markReaderIndex();

	     // Read the length field.
	     int length = buf.readInt();
	     
	     //可读字节数
	     if (buf.readableBytes() < length+8) {
	        buf.resetReaderIndex();

	        return null;
	     }

	     // There's enough bytes in the buffer. Read it.
	     ChannelBuffer frame = buf.readBytes(length);

	     // Successfully decoded a frame.  Return the decoded frame.
	     return frame;
	}

   
}