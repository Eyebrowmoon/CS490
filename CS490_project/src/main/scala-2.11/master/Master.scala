package master

import java.io.IOException
import java.net.InetAddress
import java.util.concurrent.LinkedBlockingQueue

import common._
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.Channel
import io.netty.channel.group.{ChannelGroupFuture, ChannelGroupFutureListener, DefaultChannelGroup}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.util.concurrent.GlobalEventExecutor

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import com.typesafe.scalalogging.Logger

class Master(numSlave: Int) {
  import Master._

  val logger = Logger("Master")

  private val acceptorGroup, handlerGroup = new NioEventLoopGroup()
  private val messageQueue: LinkedBlockingQueue[Message] = new LinkedBlockingQueue[Message]()
  private val samples: mutable.MutableList[Key] = mutable.MutableList.empty
  private val slaveAddressList: mutable.MutableList[String] = mutable.MutableList.empty

  private val connected = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)
  private val slaves = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)

  private var state: MasterState = MasterInitState
  private var numSampleFinishedSlave: Int = 0
  private var numComputeFinishedSlave: Int = 0

  def run(): Unit = {
    try {
      val bootstrap = new ServerBootstrap()
        .group(acceptorGroup, handlerGroup)
        .channel(classOf[NioServerSocketChannel])
        .childHandler(new MasterChannelInitializer(this))

      printInetSocketAddress()

      bootstrap.bind(port).sync()

      handleMessageLoop()
    } finally {
      acceptorGroup.shutdownGracefully()
      handlerGroup.shutdownGracefully()
    }
  }

  def interrupted: Boolean = Thread.currentThread.isInterrupted

  def handleMessageLoop(): Unit = {
    try {
      while (!interrupted) {
        val msg = messageQueue.take()
        handleMessage(msg)
      }
    } catch {
      case e: InterruptedException =>
      case e: Exception => e.printStackTrace()
    }
  }

  def terminate(): Unit = {
    logger.info("Terminate")

    Thread.currentThread().interrupt()
  }

  /* BlockingQueue is thread safe, and so this method is also thread safe */
  def addMessage(message: Message): Unit = messageQueue add message

  def tryAddToConnected(channel: Channel): Boolean = synchronized {
    if (state == MasterInitState) {
      connected add channel
    } else false
  }

  def tryAddToSlaveAndRemoveFromConnected(channel: Channel): Boolean = synchronized {
    if (numSampleFinishedSlave < numSlave) {
      numSampleFinishedSlave += 1

      (slaves add channel) && (connected remove channel)
    } else false
  }

  private def handleMessage(message: Message): Unit = state match {
    case MasterInitState => initHandleMessage(message)
    case MasterSampleState => sampleHandleMessage(message)
    case MasterComputeState => computeHandleMessage(message)
    case MasterSuccessState =>
  }

  private def initHandleMessage(message: Message) = message match {
    case SampleMessage(address, numData, keys) => handleSampleMessage(address, numData, keys)
    case _ =>
  }

  private def sampleHandleMessage(message: Message): Unit = message match {
    case PivotMessage(pivots) => handlePivotMessage(pivots)
    case _ =>
  }

  private def computeHandleMessage(message: Message): Unit = message match {
    case FileInfoMessage(files, ownerIP) => handleFileInfoMessage(FileInfoMessage(files, ownerIP))
    case DoneMessage => handleDoneMessage()
    case _ =>
  }

  private def printSlaveIP(): Unit = {
    val slaveIPConcat: String = slaveAddressList.mkString("",", ","")
    println(slaveIPConcat)
  }

  private def handlePivotMessage(pivots: List[Key]): Unit = {
    logger.info("Received PivotMessage")

    changeToComputeState()
    sendSlavesInfoMessage(pivots)
  }

  private def handleSampleMessage(address: String, numData: Long, keys: String): Unit = {
    val keyArray = stringToKeyArray(keys)

    logger.info("Received SampleMessage")

    samples ++= keyArray
    slaveAddressList += address

    if (numSampleFinishedSlave >= numSlave)
      changeToSampleState()
  }

  private def handleFileInfoMessage(fileInfoMessage: FileInfoMessage): Unit = {
    logger.info("Received FileInfoMessage")

    slaves.writeAndFlush(fileInfoMessage)
  }

  private def handleDoneMessage(): Unit = {
    logger.info("Received DoneMessage")

    numComputeFinishedSlave += 1
    if (numComputeFinishedSlave >= numSlave)
      changeToSuccessState()
  }

  private def sendSlavesInfoMessage(pivots: List[Key]): Unit = {
    logger.info("Send SlaveInfoMessage")

    val pivotString: String = pivots map { pivot =>
      pivot map {_.toChar} mkString
    } mkString

    slaves.write(SlaveInfoMessage(slaveAddressList.toArray, pivotString))
    slaves.flush()
  }

  private def startPivotCalculator(): Unit = {
    logger.info("Start PivotCalculator")

    val pivotFuture: Future[List[Key]] = PivotCalculator.getPivots(samples.toList, numSlave)
    pivotFuture onSuccess { case pivots => this.addMessage(PivotMessage(pivots)) }
  }

  private def changeToSampleState(): Unit = {
    logger.info("Change to SampleState")

    state = MasterSampleState

    printSlaveIP()
    startPivotCalculator()

    val channelGroupFuture = connected.writeAndFlush(SlaveFullMessage)
    channelGroupFuture.addListener(new ChannelGroupFutureListener{
      override def operationComplete(future: ChannelGroupFuture): Unit = connected.close()
    })
  }

  private def changeToComputeState(): Unit = {
    logger.info("Change to ComputeState")

    state = MasterComputeState
  }

  private def changeToSuccessState(): Unit = {
    logger.info("Change to SuccessState")

    state = MasterSuccessState

    slaves.writeAndFlush(TerminateMessage).sync()

    terminate()
  }

  private def printInetSocketAddress(): Unit = {
    val IPAddress: String = InetAddress.getLocalHost.getHostAddress
    println(s"$IPAddress:$port")
  }
}

object Master {
  val port: Int = 24924

  def main(args: Array[String]): Unit = {
    try{
      val numSlave: Int = args(0).toInt
      (new Master(numSlave)).run()
    } catch {
      case e: ArrayIndexOutOfBoundsException => printUsage()
      case e: IllegalArgumentException => printUsage()
      case e: Exception => e.printStackTrace()
    }
  }

  def printUsage(): Unit = println("Usage: master <# of slaves>")
}