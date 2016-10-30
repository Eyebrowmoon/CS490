package Master

import java.net.Socket

import Common._

import scala.collection.mutable

abstract class MasterState
object MasterInitState extends MasterState
object MasterSampleState extends MasterState
object MasterComputeState extends MasterState
object MasterSuccessState extends MasterState

class MasterStateManager(numSlave: Int) extends StateManager {

  val connectionListener: ConnectionListener = new ConnectionListener(this)
  val slaves: mutable.MutableList[SocketHandler] = mutable.MutableList.empty

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

  protected def handleMessage(message: Message): Unit = message match {
    case ConnectionMessage(socket) => handleConnectionMessage(socket)
  }

  private def printSlaveIP(): Unit = {
    val slaveIPConcat: String = slaves.map(_.partnerIP).mkString("",", ","")
    println(slaveIPConcat)
  }

  private def handleConnectionMessage(socket: Socket) = {
    val socketHandler: SocketHandler = new SocketHandler(socket, socketMessageHandler)
    slaves += socketHandler
    socketHandler.start()
  }

  /* Temporary handler */
  def socketMessageHandler(message: SendableMessage): Unit = message match {
    case SendableSampleMessage(numData, keys) => println(numData)
  }
}
