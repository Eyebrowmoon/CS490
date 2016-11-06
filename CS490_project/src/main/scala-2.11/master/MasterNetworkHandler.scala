package master

import common.{Message, MessageToStringEncoder, SampleMessage, SlaveFullMessage, StringToMessageDecoder}
import io.netty.channel.socket.SocketChannel
import io.netty.channel._
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import io.netty.handler.stream.ChunkedWriteHandler
import io.netty.util.CharsetUtil


class MasterNetworkHandler(master: Master) extends SimpleChannelInboundHandler[Message] {

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    if (! master.tryAddToConnected(ctx.channel)) {
      val channelFuture = ctx.writeAndFlush(SlaveFullMessage)
      channelFuture.addListener(ChannelFutureListener.CLOSE)
    }
  }

  private def handleSampleMessage(ctx: ChannelHandlerContext, message: SampleMessage) = {
    val channel = ctx.channel
    if (master tryAddToSlaveAndRemoveFromConnected channel)
      master addMessage message
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: Message) = {
    msg match {
      case SampleMessage(address, numData, sample) => handleSampleMessage(ctx, SampleMessage(address, numData, sample))
      case _ => master addMessage msg
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}

class MasterChannelInitializer(master: Master) extends ChannelInitializer[SocketChannel] {
  def initChannel(channel: SocketChannel): Unit = {
    val pipeline = channel.pipeline

    pipeline.addLast(new ChunkedWriteHandler)

    // Outbound
    pipeline.addLast(new StringEncoder(CharsetUtil.UTF_8))
    pipeline.addLast(new MessageToStringEncoder)

    // Inbound
    pipeline.addLast(new StringDecoder(CharsetUtil.UTF_8))
    pipeline.addLast(new StringToMessageDecoder)
    pipeline.addLast(new MasterNetworkHandler(master))
  }
}
