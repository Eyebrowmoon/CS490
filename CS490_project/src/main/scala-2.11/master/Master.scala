package master

import java.net.InetAddress
import java.util.concurrent.LinkedBlockingQueue

import common._
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.group.DefaultChannelGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.util.concurrent.GlobalEventExecutor

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class Master(numSlave: Int) {
  import Master._

  private val acceptorGroup, handlerGroup = new NioEventLoopGroup()
  private val messageQueue: LinkedBlockingQueue[Message] = new LinkedBlockingQueue[Message]()
  private val samples: mutable.MutableList[Key] = mutable.MutableList.empty
  private val slaveAddressList: mutable.MutableList[String] = mutable.MutableList.empty
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
    Thread.currentThread().interrupt()
  }

  /* BlockingQueue is thread safe, and so this method is also thread safe */
  def addMessage(message: Message): Unit = messageQueue add message

  def addSlave(channel: SocketChannel): Unit = slaves add channel

  protected def handleMessage(message: Message): Unit = state match {
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
    case DoneMessage => handleDoneMessage()
    case _ =>
  }

  private def printSlaveIP(): Unit = {
    val slaveIPConcat: String = slaveAddressList.mkString("",", ","")
    println(slaveIPConcat)
  }

  private def handlePivotMessage(pivots: List[Key]): Unit = {
    sendSlavesInfoMessage(pivots)
    changeToComputeState()
  }

  private def handleSampleMessage(address: String, numData: Long, keys: String): Unit = {
    val keyArray = stringToKeyArray(keys)

    samples ++= keyArray
    slaveAddressList += address
    numSampleFinishedSlave += 1

    if (numSampleFinishedSlave >= numSlave)
      changeToSampleState()
  }

  private def handleDoneMessage(): Unit = {
    numComputeFinishedSlave += 1
    if (numComputeFinishedSlave >= numSlave)
      changeToSuccessState()
  }

  private def sendSlavesInfoMessage(pivots: List[Key]): Unit = {
    val pivotString: String = pivots map { pivot =>
      pivot map {_.toChar} mkString
    } mkString

    slaves.write(SlaveInfoMessage(slaveAddressList.toArray, pivotString))
    slaves.flush()
  }

  private def startPivotCalculator(): Unit = {
    val pivotFuture: Future[List[Key]] = PivotCalculator.getPivots(samples.toList, numSlave)
    pivotFuture onSuccess  { case pivots => this.addMessage(PivotMessage(pivots)) }
  }

  private def changeToSampleState(): Unit = {
    state = MasterSampleState

    printSlaveIP()
    startPivotCalculator()
  }

  private def changeToComputeState(): Unit = {
    state = MasterComputeState
  }

  private def changeToSuccessState(): Unit = {
    state = MasterSuccessState
    terminate()
  }

  private def printInetSocketAddress(): Unit = {
    val IPAddress: String = InetAddress.getLocalHost.getHostAddress
    println(s"$IPAddress:$port")
  }

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

object Master {
  val port: Int = 24924
}