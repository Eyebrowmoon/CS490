package slave

import java.util.concurrent.LinkedBlockingQueue

import common.Message
import io.netty.channel.socket.SocketChannel
import io.netty.channel.{ChannelHandlerContext, ChannelInitializer, SimpleChannelInboundHandler}
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import io.netty.handler.stream.ChunkedWriteHandler

class SlaveChannelHandler(messageQueue: LinkedBlockingQueue[Message]) extends SimpleChannelInboundHandler[Message] {
  override def channelRead0(ctx: ChannelHandlerContext, msg: Message): Unit = {
    messageQueue add msg
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    ctx.close()
  }
}

class SlaveChannelInitializer(messageQueue: LinkedBlockingQueue[Message])
  extends ChannelInitializer[SocketChannel] {
  override def initChannel(ch: SocketChannel): Unit = {
    val pipeline = ch.pipeline()

    pipeline.addLast(new ChunkedWriteHandler)

    pipeline.addLast(new StringEncoder)

    pipeline.addLast(new StringDecoder)
    pipeline.addLast(new SlaveChannelHandler(messageQueue))
  }
}
