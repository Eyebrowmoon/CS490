package Master

import Common.{Message, StateManager}

class MasterStateManager(numSlave: Int) extends StateManager {

  val connectionListener: ConnectionListener = new ConnectionListener(this)

  override def run(): Unit = {
    init()
    super.run()
  }

  def init(): Unit = {
    connectionListener.start()
  }

  def handleMessage(message: Message): Unit = {
    Thread.currentThread.interrupt()
    connectionListener.terminate()
  }
}
