package slave

import java.io.{File, FileOutputStream}

import com.typesafe.scalalogging.Logger
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelHandlerContext, ChannelInitializer, SimpleChannelInboundHandler}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.LineBasedFrameDecoder
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import io.netty.handler.stream.ChunkedWriteHandler
import io.netty.util.CharsetUtil

class FileRequestManager(ownerIP: String, path: String) {

  val logger = Logger(s"FileRequestManager(${path})")

  val out = new FileOutputStream(new File(s"${path}_received"))
  val group = new NioEventLoopGroup()

  def run(): Unit = {
    logger.info("Start running")
    try {
      val bootstrap = new Bootstrap()
        .group(group)
        .channel(classOf[NioSocketChannel])
        .handler(new FileRequestHandlerInitializer(path, out))

      val channelFuture = bootstrap.connect(ownerIP, slavePort).sync()

      channelFuture.channel().closeFuture().sync()
    } finally {
      group.shutdownGracefully()
      out.close()
    }
    logger.info("Terminate")
  }
}

class FileRequestHandlerInitializer(path: String, out: FileOutputStream) extends ChannelInitializer[SocketChannel] {
  override def initChannel(channel: SocketChannel): Unit = {
    val pipeline = channel.pipeline

    pipeline.addLast(new StringEncoder(CharsetUtil.UTF_8))
    pipeline.addLast(new StringDecoder(CharsetUtil.UTF_8))
    pipeline.addLast(new ChunkedWriteHandler())
    pipeline.addLast(new FileRequestHandler(path, out))
  }
}

class FileRequestHandler(path: String, out: FileOutputStream) extends SimpleChannelInboundHandler[String] {

  val logger = Logger(s"FileRequestHandler(${path})")

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    logger.info("Channel active")

    ctx.writeAndFlush(path)
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: String): Unit = {
    out.write(msg.toCharArray.map{_.toByte})
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}