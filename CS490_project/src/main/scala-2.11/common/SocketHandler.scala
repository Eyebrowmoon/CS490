package common

import java.net.{SocketException}
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, SocketChannel}

import scala.annotation.tailrec
import scala.pickling.Defaults._
import scala.pickling.PicklingException
import scala.pickling.json._

class SocketHandler(socketChannel: SocketChannel, stateManager: StateManager)
  extends Thread {

  var terminated = false
  val selector = Selector.open()

  private def initSocketChannel(): Unit = {
    socketChannel.configureBlocking(false)
    socketChannel.register(selector, SelectionKey.OP_READ)
  }

  private def readFromChannel(): Option[String] = {
    selector.select(100)

    val iter = selector.selectedKeys.iterator
    readFromIter(iter)
  }

  /* has side effect due to iter.next()
   * Remove of iteration is here,
   * since it must be called after iter.next() */
  private def readFromIter(iter: java.util.Iterator[SelectionKey]): Option[String] = {
    if (!terminated && iter.hasNext) {
      val key: SelectionKey = iter.next()

      if(key.isReadable) {
        iter.remove()
        Some(readFromKey(key))
      }
      else readFromIter(iter)
    } else None
  }

 private def readFromKey(key: SelectionKey): String = {
    val buffer: ByteBuffer = ByteBuffer.allocate(1024)
    socketChannel read buffer
    buffer.clear()
    new String(buffer.array)
  }

  override def run(): Unit = {
    initSocketChannel()

    try{
      while (!terminated) {
        readFromChannel() match {
          case Some(messageString) => {
            println(messageString) // For debug. It will be removed later.
            val message = messageString.unpickle[SendableMessage]
            handleMessage(message)
          }
          case None =>
        }
      }
    } catch {
      case e: SocketException =>
      case e: PicklingException =>
      case e: Exception => e.printStackTrace()
    }
  }

  def terminate(): Unit = {
    terminated = true

    socketChannel.socket.close
    socketChannel.close
  }

  def partnerIP(): String = {
    val remoteAddress = socketChannel.getRemoteAddress.toString.substring(1)
    val colonIdx = remoteAddress.indexOf(":")

    remoteAddress.substring(0, colonIdx)
  }

  def handleMessage(message: SendableMessage): Unit = message match {
    case SendableSampleMessage(numData, sampleSize) => handleSampleMessage(numData, sampleSize)
    case SlaveInfoMessage(pivots, slaveIP, slaveNum) => stateManager.addMessage(message)
    case SendableDoneMessage => stateManager.addMessage(DoneMessage(this))
  }

  private def sendBlock(str: String): Unit = {
    val buffer = ByteBuffer.wrap(str.getBytes)
    socketChannel write buffer
    buffer.clear()
  }

  def sendMessage(message: SendableMessage): Unit = {
    val messageString = message.pickle.value
    sendBlock(messageString)
  }

  def sendString(message: String): Unit = {
    val numOfPacket: Int = Math.ceil(message.length * 1.0 / packetSize).toInt

    (0 until (numOfPacket - 1)) foreach { i =>
      val start = i * packetSize
      val block = message.substring(start, start + packetSize)
      sendBlock(block)
    }

    if (numOfPacket != 0) {
      val block = message.substring((numOfPacket - 1) * packetSize)
      sendBlock(block)
    }
  }

  private def handleSampleMessage(numData: Long, sampleSize: Int): Unit = {
    val sample = recvString(sampleSize)
    val sampleKeyArray = stringToKeyArray(sample)

    stateManager.addMessage(SampleMessage(numData, sampleKeyArray, this))
  }

  @tailrec
  private def recvBlock(): String = {
    readFromChannel() match {
      case Some(str) => str
      case None => recvBlock()
    }
  }

  private def recvString(length: Int): String = {
    var result: String = ""
    while (result.length < length)
      result += recvBlock()

    result.substring(0, length)
  }
}
