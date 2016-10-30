package Slave

import java.net.Socket

import Common._

class SlaveStateManager(masterAddress: String, inputDirs: Array[String], outputDir: String)
  extends StateManager {

  def masterIP: String = masterAddress.substring(0, masterAddress.indexOf(":"))
  def masterPort: Int = masterAddress.substring(masterAddress.indexOf(":") + 1).toInt

  val masterSocketHandler: SocketHandler = {
    new SocketHandler(new Socket(masterIP, masterPort), socketMessageHandler)
  }

  override def run() = {
    init()
    super.run()
  }

  def init(): Unit = {
    masterSocketHandler.start()
    masterSocketHandler.sendMessage(new SendableSampleMessage(0, "Test"))
  }

  def socketMessageHandler(message: SendableMessage): Unit = {}

  protected def handleMessage(message: Message): Unit = {

  }

}
