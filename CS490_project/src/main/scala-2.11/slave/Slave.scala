package slave

import java.net.InetAddress
import java.util.concurrent.LinkedBlockingQueue

import common.{DoneMessage, SampleMessage, SlaveInfoMessage, _}
import io.netty.bootstrap.Bootstrap
import io.netty.channel.Channel
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel

class Slave(masterInetSocketAddress: String, inputDirs: Array[String], outputDir: String) {

  private val fileHandler = new FileHandler(inputDirs, outputDir)
  private val messageQueue: LinkedBlockingQueue[Message] = new LinkedBlockingQueue[Message]()
  private val group = new NioEventLoopGroup()

  private var state: SlaveState = SlaveConnectState
  private var pivots: Array[Key] = _
  private var slaveIP: Array[String] = _

  def run(): Unit = {
    try {
      val bootstrap = new Bootstrap()
        .group(group)
        .channel(classOf[NioSocketChannel])
        .handler(new SlaveChannelInitializer(this))

      val channel = connectBootstrapToMaster(bootstrap)

      messageHandleLoop(channel)

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

  def interrupted: Boolean = Thread.currentThread.isInterrupted

  private def messageHandleLoop(channel: Channel): Unit = {
    try {
      while (!interrupted) {
        val msg = messageQueue.take()
        handleMessage(msg, channel)
      }
    } catch {
      case e: InterruptedException =>
      case e: Exception => e.printStackTrace()
    }
  }

  def terminate(): Unit = {
    Thread.currentThread.interrupt()
  }

  def sampleMessage(): SampleMessage = {
    val sampleString = fileHandler.sampleFromInput()

    new SampleMessage(InetAddress.getLocalHost.getHostAddress, fileHandler.dataSize, sampleString)
  }

  def addMessage(message: Message) = messageQueue add message

  protected def handleMessage(message: Message, channel: Channel): Unit = state match {
    case SlaveConnectState => connectHandleMessage(message, channel)
    case SlaveComputeState =>
    case SlaveSuccessState =>
  }

  private def connectHandleMessage(message: Message, channel: Channel) = message match {
    case SlaveFullMessage => terminate()
    case SlaveInfoMessage(slaveIP, pivots) => handleSlaveInfoMessage(slaveIP, pivots, channel: Channel)
    case _ =>
  }

  private def handleSlaveInfoMessage(slaveIP: Array[String], pivotString: String, channel: Channel): Unit = {
    this.slaveIP = slaveIP
    this.pivots = stringToKeyArray(pivotString)

    changeToComputeState(channel)
  }

  def printPivotValues(): Unit = {
    pivots foreach { pivot =>
      println(stringToHex(keyToString(pivot)))
    }
  }

  private def changeToComputeState(channel: Channel): Unit = {
    state = SlaveComputeState

    changeToSuccessState(channel) // Temporary
  }

  private def changeToSuccessState(channel: Channel): Unit = {
    state = SlaveSuccessState

    println("Key values (Test purpose): ")
    printPivotValues()
    println("")

    channel write DoneMessage
    channel flush

    terminate()
  }
}

object Slave {
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
