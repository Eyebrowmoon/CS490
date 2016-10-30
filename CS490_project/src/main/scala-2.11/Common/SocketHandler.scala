package Common

import java.io.{BufferedReader, InputStreamReader, PrintStream}
import java.net.{Socket, SocketException}
import java.nio.CharBuffer

import scala.pickling.Defaults._
import scala.pickling.json._

class SocketHandler(socket: Socket, messageHandler: SendableMessage => Unit)
  extends Thread {

  var terminated = false

  val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
  val out = new PrintStream(socket.getOutputStream)
  val charBuffer: CharBuffer = CharBuffer.allocate(4096)

  override def run(): Unit = {
    try{
      while (!terminated) {
        in read charBuffer
        val messageString = charBuffer.array.mkString("")
        val message = messageString.unpickle[SendableMessage]
        messageHandler(message)
      }
    } catch {
      // TO DO !!!
      case e: SocketException =>
      case e: Exception => e.printStackTrace()
    }
  }

  def terminate(): Unit = {
    terminated = true
    if (!socket.isClosed)
      socket.close()
  }

  def partnerIP(): String = socket.getInetAddress.toString.substring(1)

  def sendMessage(message: SendableMessage): Unit = {
    val messageString = message.pickle.value
    out.println(messageString)
  }
}
