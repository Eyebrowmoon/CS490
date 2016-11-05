package slave

import java.util.concurrent.LinkedBlockingQueue

import common.{AckMessage, _}
import io.netty.channel.Channel

abstract class SlaveState
object SlaveConnectState extends SlaveState
object SlaveComputeState extends SlaveState
object SlaveSuccessState extends SlaveState

class SlaveStateManager(channel: Channel, fileHandler: FileHandler, messageQueue: LinkedBlockingQueue[Message]) {

  var state: SlaveState = SlaveConnectState
  var slaveNum: Int = _
  var pivots: Array[Key] = _
  var slaveIP: Array[String] = _

  def interrupted: Boolean = Thread.currentThread.isInterrupted

  private def messageHandleLoop(): Unit = {
    try {
      while (!interrupted) {
        val msg = messageQueue.take()
        handleMessage(msg)
      }
    } catch {
      case e: InterruptedException =>
      case e: Exception => e.printStackTrace()
    }
  }

  def run() = {
    init()
    messageHandleLoop()
  }

  private def init(): Unit = { }

  def terminate(): Unit = {
    Thread.currentThread.interrupt()
  }

  private def sendSample(): Unit = {
    val sampleString = fileHandler.sampleFromInput()

    sendMessage(new SendableSampleMessage(fileHandler.dataSize, sampleString.length))
    sendString(sampleString)
  }

  protected def handleMessage(message: Message): Unit = state match {
    case SlaveConnectState => connectHandleMessage(message)
    case SlaveComputeState =>
    case SlaveSuccessState =>
  }

  private def connectHandleMessage(message: Message) = message match {
    case SlaveInfoMessage(slaveIP, pivots, slaveNum) => handleSlaveInfoMessage(slaveIP, pivots, slaveNum)
    case AckMessage => sendSample()
    case _ =>
  }

  private def handleSlaveInfoMessage(slaveIP: Array[String], pivotString: String, slaveNum: Int): Unit = {
    this.slaveIP = slaveIP
    this.pivots = stringToKeyArray(pivotString)
    this.slaveNum = slaveNum

    changeToComputeState()
  }

  def printPivotValues(): Unit = {
    pivots foreach { pivot =>
      println(stringToHex(keyToString(pivot)))
    }
  }

  private def changeToComputeState(): Unit = {
    state = SlaveComputeState

    changeToSuccessState() // Temporary
  }

  private def changeToSuccessState(): Unit = {
    state = SlaveSuccessState

    println("Key values (Test purpose): ")
    printPivotValues()

    sendMessage(SendableDoneMessage)
    terminate()
  }

}
