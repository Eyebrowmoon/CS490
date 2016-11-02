package master

import java.io.IOException
import java.net.{InetAddress, InetSocketAddress, Socket}
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}

import common.ConnectionMessage

class ConnectionListener(master: MasterStateManager) extends Thread {

  val IPAddress: String = InetAddress.getLocalHost.getHostAddress
  val port: Int = 24924
  val inetSocketAddress = new InetSocketAddress(IPAddress, port)

  val serverSocketChannel: ServerSocketChannel = ServerSocketChannel.open()
  val selector: Selector = Selector.open()

  def initChannel(): Unit = {
    serverSocketChannel.bind(inetSocketAddress)
    serverSocketChannel.configureBlocking(false)
    serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT)
  }

  @volatile private var terminated = false

  def printHostAddress(): Unit = println(s"$IPAddress:$port")

  def waitSlave(): Option[SocketChannel] = {
    selector.select(100)

    val iter = selector.selectedKeys.iterator
    checkKeys(iter)
  }

  def checkKeys(iter: java.util.Iterator[SelectionKey]): Option[SocketChannel] = {
    if (!terminated && iter.hasNext) {
      val key: SelectionKey = iter.next

      if (key.isAcceptable) {
        val socketChannel: SocketChannel = serverSocketChannel.accept()
        iter.remove()
        Some(socketChannel)
      } else checkKeys(iter)
    } else None
  }

  override def run(): Unit = {
    printHostAddress()
    initChannel()

    try {
      while (!terminated) {
        waitSlave() match {
          case Some(socketChannel) => addConnectionMessage(socketChannel)
          case None =>
        }
      }
    } catch {
      case e: IOException =>
    }
  }

  def terminate(): Unit = {
    terminated = true

    serverSocketChannel.socket.close()
    serverSocketChannel.close()
  }

  def addConnectionMessage(socketChannel: SocketChannel): Unit = {
    val connectionMsg = new ConnectionMessage(socketChannel)
    master.addMessage(connectionMsg)
  }
}
