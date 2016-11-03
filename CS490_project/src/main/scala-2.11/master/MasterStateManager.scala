package master

import java.net.Socket
import java.nio.channels.SocketChannel

import common._

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

abstract class MasterState
object MasterInitState extends MasterState
object MasterSampleState extends MasterState
object MasterComputeState extends MasterState
object MasterSuccessState extends MasterState

class MasterStateManager(numSlave: Int) extends StateManager {

  val connectionListener: ConnectionListener = new ConnectionListener(this)
  val connected: mutable.MutableList[SocketHandler] = mutable.MutableList.empty
  val slaves: mutable.MutableList[SocketHandler] = mutable.MutableList.empty
  val samples: mutable.MutableList[Key] = mutable.MutableList.empty

  val finishedSlaves: mutable.MutableList[SocketHandler] = mutable.MutableList.empty

  var state: MasterState = MasterInitState

  override def run(): Unit = {
    init()
    super.run()
  }

  private def init(): Unit = {
    connectionListener.start()
  }

  def terminate(): Unit = {
    if (connectionListener.isAlive) connectionListener.terminate()
    slaves filter { _.isAlive } foreach { _.terminate() }

    Thread.currentThread().interrupt()
  }

  protected def handleMessage(message: Message): Unit = state match {
    case MasterInitState => initHandleMessage(message)
    case MasterSampleState => sampleHandleMessage(message)
    case MasterComputeState => computeHandleMessage(message)
    case MasterSuccessState =>
  }

  private def initHandleMessage(message: Message) = message match {
    case ConnectionMessage(socket) => handleConnectionMessage(socket)
    case SampleMessage(numData, keys, handler) => handleSampleMessage(numData, keys, handler)
    case _ =>
  }

  private def sampleHandleMessage(message: Message): Unit = message match {
    case PivotMessage(pivots) => handlePivotMessage(pivots)
    case _ =>
  }

  private def computeHandleMessage(message: Message): Unit = message match {
    case DoneMessage(handler) => handleDoneMessage(handler)
    case _ =>
  }

  private def printSlaveIP(): Unit = {
    val slaveIPConcat: String = slaves.map(_.partnerIP).mkString("",", ","")
    println(slaveIPConcat)
  }

  private def handleConnectionMessage(socketChannel: SocketChannel) = {
    val socketHandler: SocketHandler = new SocketHandler(socketChannel, this)
    connected += socketHandler
    socketHandler.start()
  }

  private def handlePivotMessage(pivots: List[Key]): Unit = {
    sendSlavesInfoMessage(pivots)
    changeToComputeState()
  }

  private def handleSampleMessage(numData: Long, keys: Array[Key], handler: SocketHandler): Unit = {
    slaves += handler
    samples ++= keys

    if (slaves.length >= numSlave)
      changeToSampleState()
  }

  private def handleDoneMessage(handler: SocketHandler): Unit = {
    finishedSlaves += handler

    if (finishedSlaves.length >= numSlave)
      changeToSuccessState()
  }

  private def sendSlavesInfoMessage(pivots: List[Key]): Unit = {
    val slaveIPList: Array[String] = slaves.map(_.partnerIP).toArray
    val pivotString: String = pivots map { pivot =>
      pivot map {_.toChar} mkString
    } mkString

    slaves.indices foreach { i =>
      slaves(i).sendMessage(SlaveInfoMessage(slaveIPList, pivotString, i))
    }
  }

  private def terminateSocketHandlersExceptSlaves(): Unit = (connected diff slaves) foreach { _.terminate() }

  private def startPivotCalculator(): Unit = {
    val pivotFuture: Future[List[Key]] = PivotCalculator.getPivots(samples.toList, numSlave)
    pivotFuture onSuccess  { case pivots => this.addMessage(PivotMessage(pivots)) }
  }

  private def changeToSampleState(): Unit = {
    terminateSocketHandlersExceptSlaves()
    printSlaveIP()
    startPivotCalculator()

    state = MasterSampleState
  }

  private def changeToComputeState(): Unit = {
    state = MasterComputeState
  }

  private def changeToSuccessState(): Unit = {
    state = MasterSuccessState

    terminate()
  }
}
