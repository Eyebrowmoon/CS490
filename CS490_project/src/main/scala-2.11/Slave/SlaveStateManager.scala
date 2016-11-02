package slave

import java.io.File
import java.net.Socket

import common._

class SlaveStateManager(masterAddress: String, inputDirs: Array[String], outputDir: String)
  extends StateManager {

  def masterIP: String = masterAddress.substring(0, masterAddress.indexOf(":"))
  def masterPort: Int = masterAddress.substring(masterAddress.indexOf(":") + 1).toInt

  val masterSocketHandler: SocketHandler = new SocketHandler(new Socket(masterIP, masterPort), this)
  val fileHandler = new FileHandler

  val inputFileList: List[File] = inputDirs.toList.flatMap { fileHandler.getListOfFiles(_) }
  val dataSize: Long = inputFileList.map{ _.length }.sum
  val sampleString = sampleFromInput()

  override def run() = {
    init()
    super.run()
  }

  def init(): Unit = {
    masterSocketHandler.start()
    masterSocketHandler.sendMessage(new SendableSampleMessage(dataSize, sampleString))
  }

  def sampleFromInput(): String = {
    val sampleSize: Long = List[Long](MAX_SAMPLE_SIZE, dataSize).min
    val sampleRatio: Double = sampleSize * 1.0 / dataSize
    val sampleStrings: List[String] = inputFileList map fileHandler.sampleSingleFile(sampleRatio)

    sampleStrings.mkString
  }

  protected def handleMessage(message: Message): Unit = { }

}
