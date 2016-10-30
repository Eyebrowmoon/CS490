package Master

import java.io.IOException
import java.net.{InetAddress, ServerSocket, Socket}

import Common.ConnectionMessage

class ConnectionListener(master: MasterStateManager) extends Thread {
  val IPAddress: String = InetAddress.getLocalHost.getHostAddress
  val port: Int = 24924
  val serverSocket: ServerSocket = new ServerSocket(port)

  @volatile private var terminated = false

  def printHostAddress(): Unit = println(s"$IPAddress:$port")

  override def run(): Unit = {
    printHostAddress()

    try {
      while (!terminated) {
        val socket = serverSocket.accept()
        addConnectionMessage(socket)
      }
    } catch {
      case e: IOException =>
    }
  }

  def terminate(): Unit = {
    terminated = true

    if (!serverSocket.isClosed)
      serverSocket.close()
  }

  def addConnectionMessage(socket: Socket): Unit = {
    val connectionMsg = new ConnectionMessage(socket)
    master.addMessage(connectionMsg)
  }
}
