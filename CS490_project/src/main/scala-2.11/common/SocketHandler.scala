package common

import java.io.{BufferedReader, InputStreamReader, PrintStream}
import java.net.{Socket, SocketException}
import java.nio.CharBuffer
import java.util

import scala.pickling.Defaults._
import scala.pickling.PicklingException
import scala.pickling.json._

class SocketHandler(socket: Socket, stateManager: StateManager)
  extends Thread {

  var terminated = false

  val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
  val out = new PrintStream(socket.getOutputStream)
  val charBuffer: CharBuffer = CharBuffer.allocate(packetSize)

  def eraseBuffer(): Unit = util.Arrays.fill(charBuffer.array, '\0')

  def readToBuffer(): Int = {
    charBuffer.clear()
    in read charBuffer
  }

  override def run(): Unit = {
    try{
      while (!terminated) {
        val size = readToBuffer()
        val messageString = charBuffer.array.slice(0, size).mkString

        val message = messageString.unpickle[SendableMessage]
        handleMessage(message)
      }
    } catch {
      case e: SocketException =>
      case e: PicklingException =>
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
    out.print(messageString)
  }

  def handleMessage(message: SendableMessage): Unit = message match {
    case SendableSampleMessage(numData, sampleSize) => handleSampleMessage(numData, sampleSize)
    case SlaveInfoMessage(pivots, slaveIP, slaveNum) => stateManager.addMessage(message)
    case SendableDoneMessage => stateManager.addMessage(DoneMessage(this))
  }

  def sendString(message: String): Unit = {
    val numOfPacket: Int = Math.ceil(message.length * 1.0 / packetSize).toInt

    (0 until (numOfPacket - 1)) foreach { i =>
      val start = i * packetSize
      val block = message.substring(start, start + packetSize)
      out print block
    }

    if (numOfPacket != 0){
      val block = message.substring((numOfPacket - 1) * packetSize)
      out print block
    }
  }

  private def handleSampleMessage(numData: Long, sampleSize: Int): Unit = {
    val sample = recvString(sampleSize)
    val sampleKeyArray = stringToKeyArray(sample)

    stateManager.addMessage(SampleMessage(numData, sampleKeyArray, this))
  }

  private def recvString(length: Int): String = {
    var result: String = ""
    val numPacket: Int = Math.ceil(length * 1.0 / packetSize).toInt

    (0 until numPacket) foreach { i =>
      readToBuffer()
      result += charBuffer.array.mkString
    }

    result.substring(0, length)
  }
}
