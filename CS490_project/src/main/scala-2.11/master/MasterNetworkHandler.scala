package master

import common.Message
import io.netty.channel.group.DefaultChannelGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter, ChannelInitializer, SimpleChannelInboundHandler}
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import io.netty.handler.stream.ChunkedWriteHandler
import io.netty.util.concurrent.GlobalEventExecutor

class MasterNetworkHandler(master: Master) extends SimpleChannelInboundHandler[Message] {

  override def channelRead0(ctx: ChannelHandlerContext, msg: Message) = {
    master addMessage msg
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}

class MasterChannelInitializer(master: Master) extends ChannelInitializer[SocketChannel] {
  val connected = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)

  def initChannel(channel: SocketChannel): Unit = {
    val pipeline = channel.pipeline

    pipeline.addLast(new ChunkedWriteHandler)

    pipeline.addLast(new StringEncoder)

    pipeline.addLast(new StringDecoder)
    pipeline.addLast(new MasterNetworkHandler(master))

    connected add channel
  }
}
