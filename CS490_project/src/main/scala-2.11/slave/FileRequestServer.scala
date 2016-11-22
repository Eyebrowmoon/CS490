package slave

import java.io.{File, FileInputStream, RandomAccessFile}
import java.nio.{BufferUnderflowException, ByteBuffer}
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

  def terminate(): Unit = channelOption match {
    case Some(channel) => channel.close()
    case None =>
  }
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

  val logger = Logger("FileRequestServerHandler")
  val buffer = ByteBuffer.allocateDirect(4096)

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    logger.info("Channel active")
  }

  def readEntryFromChannel(cin: FileChannel): Array[Byte] = {
    val size = readToBuffer(cin)
    if (size > 0) {
      val array: Array[Byte] = new Array[Byte](size)
      buffer.get(array)
      array
    } else new Array[Byte](0)
  }

  def readToBuffer(cin: FileChannel): Int = {
    buffer.clear()
    val result = cin.read(buffer)
    buffer.flip()

    result
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: String): Unit = {
    logger.info(s"File Request - $msg")

    val file = new File(msg)
    val raf = new RandomAccessFile(file, "r")
    val cin = raf.getChannel

    val content = readEntryFromChannel(cin)
    val writeFuture = ctx.writeAndFlush(content)

    writeFuture.addListener(new ChannelFutureListener {
      override def operationComplete(future: ChannelFuture): Unit = {
        val content = readEntryFromChannel(cin)
        if (content.length > 0) {
          val future = ctx.writeAndFlush(content)
          future.addListener(this)
        } else {
          raf.close()
          cin.close()
          file.delete()
          ctx.close()
        }
      }
    })
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}