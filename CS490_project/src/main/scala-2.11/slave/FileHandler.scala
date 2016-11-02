package slave

import java.io._
import java.nio.{CharBuffer}

import common._

class FileHandler {

  val sampleBuffer: CharBuffer = CharBuffer.allocate(100)

  def getListOfFiles(dirName: String): List[File] = {
    val dir = new File(dirName)
    if (dir.exists && dir.isDirectory) dir.listFiles.filter(_.isFile).toList
    else List[File]()
  }

  def sampleSingleFile(sampleRatio: Double)(file:File): String = {
    var inStream: BufferedReader = null
    var readSize: Long = 0
    var result = ""
    val sampleSize: Long = (sampleRatio * file.length).toLong

    try {
      inStream = new BufferedReader(new FileReader(file))
      while(readSize < sampleSize) {
        inStream read sampleBuffer
        result += new String(sampleBuffer.array.slice(0, keyLength))
        readSize += 10
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (inStream != null) inStream.close()
    }

    result
  }
}
