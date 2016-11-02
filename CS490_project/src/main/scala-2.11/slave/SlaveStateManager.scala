package slave

import java.io.File
import java.net.{InetSocketAddress, Socket}
import java.nio.channels.{SelectionKey, SocketChannel}

import common._

abstract class SlaveState
object SlaveConnectState extends SlaveState
object SlaveComputeState extends SlaveState
object SlaveSuccessState extends SlaveState

class SlaveStateManager(masterAddress: String, inputDirs: Array[String], outputDir: String)
  extends StateManager {

  val masterIP: String = masterAddress.substring(0, masterAddress.indexOf(":"))
  val masterPort: Int = masterAddress.substring(masterAddress.indexOf(":") + 1).toInt
  val masterInetSocketAddress = new InetSocketAddress(masterIP, masterPort)

  val masterSocketHandler: SocketHandler = new SocketHandler(openSocketChannel, this)
  val fileHandler = new FileHandler

  val inputFileList: List[File] = inputDirs.toList.flatMap { fileHandler.getListOfFiles(_) }
  val dataSize: Long = inputFileList.map{ _.length }.sum

  var state: SlaveState = SlaveConnectState
  var slaveNum: Int = _
  var pivots: Array[Key] = _
  var slaveIP: Array[String] = _

  override def run() = {
    init()
    super.run()
  }

  private def init(): Unit = {
    masterSocketHandler.start()
  }

  def terminate(): Unit = {
    masterSocketHandler.terminate()
    Thread.currentThread.interrupt()
  }

  def openSocketChannel(): SocketChannel = {
    val socketChannel = SocketChannel.open()
    socketChannel.connect(masterInetSocketAddress)
    socketChannel
  }

  private def sendSample(): Unit = {
    val sampleString = sampleFromInput()

    masterSocketHandler.sendMessage(new SendableSampleMessage(dataSize, sampleString.length))
    masterSocketHandler.sendString(sampleString)
  }

  private def sampleFromInput(): String = {
    val sampleSize: Int = List[Long](MAX_SAMPLE_SIZE, dataSize).min.toInt
    val sampleRatio: Double = sampleSize * 1.0 / dataSize
    val sampleStrings: List[String] = inputFileList map fileHandler.sampleSingleFile(sampleRatio)

    sampleStrings.mkString
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

    masterSocketHandler.sendMessage(SendableDoneMessage)
    terminate()
  }

}
