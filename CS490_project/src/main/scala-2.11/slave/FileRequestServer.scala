package slave

import java.io.{File, RandomAccessFile}

import com.typesafe.scalalogging.Logger
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.LineBasedFrameDecoder
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import io.netty.handler.stream.{ChunkedFile, ChunkedWriteHandler}
import io.netty.util.CharsetUtil

class FileRequestServer extends Thread {

  val logger = Logger("FileRequestServer")

  val bossGroup, workerGroup = new NioEventLoopGroup()
  var channelOption: Option[Channel] = None

  override def run(): Unit = {
    logger.info("Start running")
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
    logger.info("Terminate")
  }

  def terminate(): Unit = channelOption match {
    case Some(channel) => channel.close()
    case None =>
  }
}

class FileRequestServerInitializer extends ChannelInitializer[SocketChannel] {
  override def initChannel(channel: SocketChannel): Unit = {
    val pipeline = channel.pipeline

    pipeline.addLast(new StringEncoder(CharsetUtil.UTF_8))
    pipeline.addLast(new StringDecoder(CharsetUtil.UTF_8))
    pipeline.addLast(new ChunkedWriteHandler())
    pipeline.addLast(new FileRequestServerHandler())
  }
}

class FileRequestServerHandler extends SimpleChannelInboundHandler[String] {

  val logger = Logger("FileRequestServerHandler")

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    logger.info("Channel active")
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: String): Unit = {
    logger.info(s"File Request - $msg")

    val file = new File(msg)

    val writeFuture = ctx.write(new ChunkedFile(file))
    writeFuture.addListener(new ChannelFutureListener {
      override def operationComplete(future: ChannelFuture): Unit = {
        file.delete()
        ctx.close()
      }
    })
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}