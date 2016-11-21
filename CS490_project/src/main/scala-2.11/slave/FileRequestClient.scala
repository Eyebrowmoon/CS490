package slave

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import com.typesafe.scalalogging.Logger
import io.netty.bootstrap.Bootstrap
import io.netty.channel.{ChannelHandlerContext, ChannelInitializer, SimpleChannelInboundHandler}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import io.netty.handler.stream.ChunkedWriteHandler
import io.netty.util.CharsetUtil

class FileRequestManager(ownerIP: String, path: String) {

  val raf = new RandomAccessFile(path, "w")
  val cin = raf.getChannel

  val group = new NioEventLoopGroup()

  def run(): Unit = {
    try {
      val bootstrap = new Bootstrap()
        .group(group)
        .channel(classOf[NioSocketChannel])
        .handler(new FileRequestHandlerInitializer(path, cin))

      bootstrap.bind(ownerIP, slavePort).sync().channel().closeFuture().sync()
    } finally {
      group.shutdownGracefully()
      raf.close()
      cin.close()
    }
  }
}

class FileRequestHandlerInitializer(path: String, cin: FileChannel) extends ChannelInitializer[SocketChannel] {
  override def initChannel(channel: SocketChannel): Unit = {
    val pipeline = channel.pipeline

    pipeline.addLast(new ChunkedWriteHandler())

    pipeline.addLast(new StringEncoder(CharsetUtil.UTF_8))

    pipeline.addLast(new StringDecoder(CharsetUtil.UTF_8))
    pipeline.addLast(new FileRequestHandler(path, cin))
  }
}

class FileRequestHandler(path: String, cin: FileChannel) extends SimpleChannelInboundHandler[String] {

  val logger = Logger(s"FileRequestHandler(${path})")

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    logger.info("Channel active")

    ctx.writeAndFlush(path)
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: String): Unit = {
    val byteBuffer = ByteBuffer.wrap(msg.getBytes())
    cin.write(byteBuffer)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}