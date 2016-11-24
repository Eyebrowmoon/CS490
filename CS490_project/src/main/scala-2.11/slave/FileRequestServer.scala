package slave

import java.io.{File}
import java.nio.{ByteBuffer}
import java.nio.channels.FileChannel

import com.typesafe.scalalogging.Logger
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.bytes.{ByteArrayEncoder}
import io.netty.handler.codec.string.{StringDecoder}

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

  def terminate(): Unit = channelOption foreach { _.close() }
}

class FileRequestServerInitializer extends ChannelInitializer[SocketChannel] {
  override def initChannel(channel: SocketChannel): Unit = {
    val pipeline = channel.pipeline

    pipeline.addLast(new ByteArrayEncoder())
    pipeline.addLast(new StringDecoder())
    pipeline.addLast(new FileRequestServerHandler())
  }
}

class FileRequestServerHandler extends SimpleChannelInboundHandler[String] {

  import FileHandler._

  val logger = Logger("FileRequestServerHandler")
  val buffer = ByteBuffer.allocateDirect(4096)

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    logger.info("Channel active")
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: String): Unit = {
    logger.info(s"File Request - $msg")
    val rafHandler = new RandomAccessFileHandler(msg, "r")

    def tryWriteWithListenerOrElse(cin: FileChannel, listener: ChannelFutureListener)(elseBlock: => Unit): Unit = {
      val content = readByteArray(cin, buffer)
      if (content.length > 0) ctx.writeAndFlush(content).addListener(listener)
      else elseBlock
    }

    def clear(): Unit = {
      new File(msg).delete()
      rafHandler.close()
      ctx.close()
    }

    rafHandler.execute { case (raf, cin) =>
      val listener = new ChannelFutureListener {
        override def operationComplete(future: ChannelFuture): Unit = {
          tryWriteWithListenerOrElse(cin, this)(clear)
        }
      }

      tryWriteWithListenerOrElse(cin, listener)()
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}