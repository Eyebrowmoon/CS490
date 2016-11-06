package slave

import common.{Message, MessageToStringEncoder, StringToMessageDecoder}
import io.netty.channel.socket.SocketChannel
import io.netty.channel.{ChannelHandlerContext, ChannelInitializer, SimpleChannelInboundHandler}
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import io.netty.handler.stream.{ChunkedWriteHandler}
import io.netty.util.CharsetUtil

class SlaveChannelHandler(slave: Slave) extends SimpleChannelInboundHandler[Message] {
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    val sampleMessage = slave.sampleMessage

    ctx.writeAndFlush(sampleMessage)
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: Message): Unit = {
    slave addMessage msg
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}

class SlaveChannelInitializer(slave: Slave)
  extends ChannelInitializer[SocketChannel] {
  override def initChannel(ch: SocketChannel): Unit = {
    val pipeline = ch.pipeline()

    pipeline
      .addLast(new ChunkedWriteHandler)

    pipeline // Outbound
      .addLast(new StringEncoder(CharsetUtil.UTF_8))
      .addLast(new MessageToStringEncoder)

    pipeline // Inbound
      .addLast(new StringDecoder(CharsetUtil.UTF_8))
      .addLast(new StringToMessageDecoder)
      .addLast(new SlaveChannelHandler(slave))
  }
}
