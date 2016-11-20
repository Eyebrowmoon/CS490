package slave

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import com.typesafe.scalalogging.Logger
import common._

class FileHandler(inputDirs: Array[String], val outputDir: String) {

  val logger = Logger("FileHandler")

  val inputFileList: List[File] = inputDirs.toList.flatMap(getListOfFiles)
  val dataSize: Long = inputFileList.map{ _.length }.sum

  private val entryBuffer: ByteBuffer = ByteBuffer.allocateDirect(entryLength)

  private def getListOfFiles(dirName: String): List[File] = {
    val dir = new File(dirName)
    if (dir.exists && dir.isDirectory) dir.listFiles.filter(_.isFile).toList
    else List[File]()
  }

  private def readToEntryBuffer(cin: FileChannel): Unit = {
    entryBuffer.clear()
    cin read entryBuffer
    entryBuffer.flip()
  }

  private def readKeysFromFile(raf: RandomAccessFile, cin: FileChannel, numKeys: Int): String = {
    var result = ""

    (0 until numKeys) foreach { i =>
      val keyArray: Array[Byte] = new Array[Byte](keyLength)

      raf.seek(i * entryLength)
      readToEntryBuffer(cin)

      entryBuffer.get(keyArray)
      result += keyArray.map{_.toChar}.mkString
    }

    result
  }

  // Assume uniform distribution
  private def sampleSingleFile(sampleRatio: Double)(file: File): String = {
    val numKeys: Int = (sampleRatio * file.length).toInt / 10

    logger.info(s"Sample ${file.getCanonicalPath}")

    val raf = new RandomAccessFile(file.getCanonicalPath, "r")
    val cin = raf.getChannel

    val result = readKeysFromFile(raf, cin, numKeys)

    raf.close()
    cin.close()

    result
  }

  def sampleFromInput(): String = {
    logger.info("Sampling start")

    val sampleSize: Int = List[Long](MAX_SAMPLE_SIZE, dataSize).min.toInt
    val sampleRatio: Double = sampleSize * 1.0 / dataSize
    val sampleStrings: List[String] = inputFileList map sampleSingleFile(sampleRatio)

    logger.info("Sampling end")

    sampleStrings.mkString
  }

  def readEntryFromChannel(cin: FileChannel): Entry = {
    val entryArray: Entry = new Array[Byte](entryLength)

    readToEntryBuffer(cin)
    entryBuffer.get(entryArray)

    entryArray
  }

  def readEntries(file: File, startPos: Long, numEntries: Int): Array[Entry] = {
    val arraySize = List(numEntries, ((file.length - startPos) / entryLength).toInt).min
    val result: Array[Entry] = new Array[Entry](arraySize)

    logger.info(s"Read $arraySize entries from ${file.getCanonicalPath}-$startPos")

    val raf = new RandomAccessFile(file.getCanonicalPath, "r")
    val cin = raf.getChannel

    raf.seek(startPos)
    result.indices foreach { i => result(i) = readEntryFromChannel(cin) }

    raf.close()
    cin.close()

    result
  }

  def saveEntriesToFile(entries: Array[Entry], fileName: String): Unit = {
    val out = new FileOutputStream(fileName)

    entries foreach { entry =>
      out.write(entry)
    }

    out.close()
  }
}
