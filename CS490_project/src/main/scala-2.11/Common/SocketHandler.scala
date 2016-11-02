package common

import java.io.{BufferedReader, InputStreamReader, PrintStream}
import java.net.{Socket, SocketException}
import java.nio.CharBuffer

import scala.pickling.Defaults._
import scala.pickling.json._

class SocketHandler(socket: Socket, stateManager: StateManager)
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
        handleMessage(message)

        charBuffer.clear()
      }
    } catch {
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

  def stringToKeyArray(keyString: String): Array[Key] = {
    val byteArray = keyString.getBytes
    val arrayLength = keyString.length / keyLength
    val keyArray = new Array[Key](arrayLength)

    (0 until arrayLength) foreach { i =>
      keyArray(i) = byteArray.slice(i * keyLength, (i+1) * keyLength)
    }

    keyArray
  }

  def handleMessage(message: SendableMessage): Unit = message match {
    case SendableSampleMessage(numData, keys) =>
      stateManager.addMessage(new SampleMessage(numData, stringToKeyArray(keys), this))
  }
}
