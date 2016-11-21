package slave

import java.io.RandomAccessFile

import com.typesafe.scalalogging.Logger
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import io.netty.handler.stream.{ChunkedFile, ChunkedWriteHandler}
import io.netty.util.CharsetUtil

class FileRequestServer extends Thread {

  val bossGroup, workerGroup = new NioEventLoopGroup()
  var channelOption: Option[Channel] = None

  override def run(): Unit = {
    try {
      val bootstrap = new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .childHandler(new FileRequestServerInitializer())

      val channelFuture = bootstrap.bind(slavePort).sync()
      channelOption = Some(channelFuture.channel)

      channelFuture.channel.closeFuture().sync()
    } finally {
      bossGroup.shutdownGracefully()
      workerGroup.shutdownGracefully()
    }
  }

  def terminate(): Unit = channelOption match {
    case Some(channel) => channel.close()
    case None =>
  }
}

class FileRequestServerInitializer extends ChannelInitializer[SocketChannel] {
  override def initChannel(channel: SocketChannel): Unit = {
    val pipeline = channel.pipeline

    pipeline.addLast(new ChunkedWriteHandler())

    pipeline.addLast(new StringEncoder(CharsetUtil.UTF_8))

    pipeline.addLast(new StringDecoder(CharsetUtil.UTF_8))
    pipeline.addLast(new FileRequestServerHandler())
  }
}

class FileRequestServerHandler extends SimpleChannelInboundHandler[String] {

  val logger = Logger("FileRequestServerHandler")

  private def closeRafOption(rafOption: Option[RandomAccessFile]): Unit = rafOption match {
    case Some(raf) => raf.close()
    case None =>
  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    logger.info("Channel active")
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: String): Unit = {
    var rafOption: Option[RandomAccessFile] = None

    logger.info(s"File Request - $msg")

    try {
      val raf = new RandomAccessFile(msg, "r")
      rafOption = Some(raf)

      val writeFuture = ctx.writeAndFlush(new ChunkedFile(raf))
      writeFuture.addListener(ChannelFutureListener.CLOSE)
    } finally {
      closeRafOption(rafOption)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}