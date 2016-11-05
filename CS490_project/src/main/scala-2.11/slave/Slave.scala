package slave

import java.util.concurrent.LinkedBlockingQueue

import common.Message
import io.netty.bootstrap.Bootstrap
import io.netty.channel.Channel
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel

class Slave(masterInetSocketAddress: String, inputDirs: Array[String], outputDir: String) {

  val messageQueue: LinkedBlockingQueue[Message] = new LinkedBlockingQueue[Message]()
  val group = new NioEventLoopGroup()

  def run(): Unit = {
    try {
      val bootstrap = new Bootstrap()
        .group(group)
        .channel(classOf[NioSocketChannel])
        .handler(new SlaveChannelInitializer(messageQueue))

      val channel = connectBootstrapToMaster(bootstrap)

      val fileHandler = new FileHandler(inputDirs, outputDir)
      val slaveStateManager = new SlaveStateManager(channel, fileHandler, messageQueue)


    } finally {
      group.shutdownGracefully()
    }
  }

  def connectBootstrapToMaster(bootstrap: Bootstrap): Channel = {
    val colonIdx = masterInetSocketAddress.indexOf(":")
    val masterIP: String = masterInetSocketAddress.substring(0, colonIdx)
    val masterPort: Int = masterInetSocketAddress.substring(colonIdx + 1).toInt

    bootstrap.connect(masterIP, masterPort).channel()
  }

  def main(args:Array[String]): Unit = {
    try {
      val masterInetSocketAddress = args(0)
      val inputDirs = parseInputDirs(args)
      val outputDirs = parseOutputDir(args)

      (new Slave(masterInetSocketAddress, inputDirs, outputDirs)).run()
    } catch {
      case e: ArrayIndexOutOfBoundsException => printUsage()
      case e: IllegalArgumentException => printUsage()
      case e: Exception => e.printStackTrace()
    }
  }

  def parseInputDirs(args: Array[String]): Array[String] = {
    val inputOption: Int = args.indexOf("-I")
    val outputOption: Int = args.indexOf("-O")

    if (inputOption < 0 || outputOption < 0) throw new IllegalArgumentException
    else {
      val inputEnd = if (inputOption < outputOption) outputOption else args.length
      val inputDirectories = args.slice(inputOption + 1, inputEnd)

      if (inputDirectories.isEmpty) throw new IllegalArgumentException
      inputDirectories
    }
  }

  def parseOutputDir(args: Array[String]): String = {
    val outputOption: Int = args.indexOf("-O")
    args(outputOption + 1)
  }

  def printUsage(): Unit = {
    println("Usage: slave <master IP:Port> -I" +
      " <input directory> <input directory> ... <input directory>" +
      " -O <output directory>")
  }
}
