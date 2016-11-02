package master

import java.net.Socket

import common._

import scala.collection.mutable

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
  }

  private def initHandleMessage(message: Message) = message match {
    case ConnectionMessage(socket) => handleConnectionMessage(socket)
    case SampleMessage(numData, keys, handler) => handleSampleMessage(numData, keys, handler)
  }

  private def sampleHandleMessage(message: Message) = { terminate() }

  private def printSlaveIP(): Unit = {
    val slaveIPConcat: String = slaves.map(_.partnerIP).mkString("",", ","")
    println(slaveIPConcat)
  }

  private def handleConnectionMessage(socket: Socket) = {
    val socketHandler: SocketHandler = new SocketHandler(socket, this)
    connected += socketHandler
    socketHandler.start()
  }

  private def changeToSampleState(): Unit = {
    (connected diff slaves) foreach { _.terminate() }
    printSlaveIP()
    state = MasterSampleState
  }

  private def handleSampleMessage(numData: Long, keys: Array[Key], handler: SocketHandler): Unit = {
    slaves += handler
    samples ++= keys

    if (slaves.length >= numSlave)
      changeToSampleState()
  }
}
