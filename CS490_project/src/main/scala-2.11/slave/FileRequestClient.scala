package slave

import java.io.{BufferedOutputStream}

import com.typesafe.scalalogging.Logger
import io.netty.bootstrap.Bootstrap
import io.netty.channel.{ChannelHandlerContext, ChannelInitializer, SimpleChannelInboundHandler}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.bytes.ByteArrayDecoder
import io.netty.handler.codec.string.StringEncoder
import io.netty.util.CharsetUtil

class FileRequestManager(ownerIP: String, path: String) {

  val logger = Logger(s"FileRequestManager(${path})")
  val group = new NioEventLoopGroup()

  def run(): Unit = {
    logger.debug("Start running")
    try {
      FileHandler.writeFile(path) { out =>
        val bootstrap = new Bootstrap()
          .group(group)
          .channel(classOf[NioSocketChannel])
          .handler(new FileRequestHandlerInitializer(path, out))

        val channelFuture = bootstrap.connect(ownerIP, slavePort).sync()

        channelFuture.channel().closeFuture().sync()
      }
    } finally {
      group.shutdownGracefully()
    }
    logger.debug("Terminate")
  }
}

class FileRequestHandlerInitializer(path: String, out: BufferedOutputStream) extends ChannelInitializer[SocketChannel] {
  override def initChannel(channel: SocketChannel): Unit = {
    val pipeline = channel.pipeline

    pipeline.addLast(new StringEncoder(CharsetUtil.UTF_8))
    pipeline.addLast(new ByteArrayDecoder())
    pipeline.addLast(new FileRequestHandler(path, out))
  }
}

class FileRequestHandler(path: String, out: BufferedOutputStream) extends SimpleChannelInboundHandler[Array[Byte]] {

  val logger = Logger(s"FileRequestHandler(${path})")

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    logger.debug("Channel active")

    ctx.writeAndFlush(path)
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: Array[Byte]): Unit = {
    out.write(msg)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}

