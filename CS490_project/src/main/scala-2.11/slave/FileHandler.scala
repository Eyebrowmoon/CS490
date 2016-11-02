package slave

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import common._

class FileHandler {

  val entryBuffer: ByteBuffer = ByteBuffer.allocateDirect(entryLength)

  def getListOfFiles(dirName: String): List[File] = {
    val dir = new File(dirName)
    if (dir.exists && dir.isDirectory) dir.listFiles.filter(_.isFile).toList
    else List[File]()
  }

  def readToEntryBuffer(cin: FileChannel): Unit = {
    entryBuffer.clear()
    cin read entryBuffer
    entryBuffer.flip()
  }

  def readKeysFromFile(raf: RandomAccessFile, cin: FileChannel, numKeys: Int): String = {
    var result = ""

    (0 until numKeys) foreach { i =>
      val keyArray: Array[Byte] = new Array[Byte](10)

      raf.seek(i * entryLength)
      readToEntryBuffer(cin)

      entryBuffer.get(keyArray)
      result += keyArray.map{_.toChar}.mkString
    }

    result
  }

  // Assume uniform distribution
  def sampleSingleFile(sampleRatio: Double)(file: File): String = {
    val numKeys: Int = (sampleRatio * file.length).toInt / 10

    val raf = new RandomAccessFile(file.getCanonicalPath, "r")
    val cin = raf.getChannel

    val result = readKeysFromFile(raf, cin, numKeys)

    raf.close()
    cin.close()

    result
  }
}
