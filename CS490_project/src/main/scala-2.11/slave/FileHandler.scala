package slave

import java.io._
import java.nio.CharBuffer

import common._

class FileHandler {

  val sampleBuffer: CharBuffer = CharBuffer.allocate(100)

  def getListOfFiles(dirName: String): List[File] = {
    val dir = new File(dirName)
    if (dir.exists && dir.isDirectory) dir.listFiles.filter(_.isFile).toList
    else List[File]()
  }

  def readKeysFromFile(inStream: BufferedReader, numKeys: Int): String = {
    var result = ""

    (0 until numKeys) foreach { _ =>
      inStream read sampleBuffer
      result += new String(sampleBuffer.array.slice(0, keyLength))
      sampleBuffer.clear
    }

    result
  }

  // Assume uniform distribution
  def sampleSingleFile(sampleRatio: Double)(file:File): String = {
    val numKeys: Int = (sampleRatio * file.length).toInt / 10
    var inStream: BufferedReader = null
    var result = ""

    try {
      inStream = new BufferedReader(new FileReader(file))
      result = readKeysFromFile(inStream, numKeys)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (inStream != null) inStream.close()
    }

    result
  }
}
