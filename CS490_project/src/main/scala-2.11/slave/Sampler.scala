package slave

import common._
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import com.typesafe.scalalogging.Logger

object Sampler {

  val logger = Logger("Sampler")
  val entryBuffer = ByteBuffer.allocateDirect(entryLength)

  private def readKeysFromFile(raf: RandomAccessFile, cin: FileChannel, numKeys: Int): String = {
    val interval = raf.length / numKeys / entryLength * entryLength
    val entryReader = new EntryReader(raf, cin)

    (0 until numKeys) map { i =>
      entryReader.readEntryFrom(i * interval).slice(0, keyLength).map{_.toChar}.mkString
    } mkString
  }

  private def sampleSingleFile(sampleRatio: Double)(path: String): String = {
    logger.info(s"Sample $path")

    new RandomAccessFileHandler(path, "r").execute { case (raf, cin) =>
      val numKeys: Int = (sampleRatio * raf.length).toInt / entryLength
      readKeysFromFile(raf, cin, numKeys)
    }
  }

  def sampleFromInput(inputFilePaths: Vector[String]): (String, Long) = {
    logger.info("Sampling start")

    val dataSize: Long = inputFilePaths.map{ FileHandler.getFileSize }.sum
    val sampleSize: Int = List[Long](MAX_SAMPLE_SIZE, dataSize).min.toInt
    val sampleRatio: Double = sampleSize * 1.0 / dataSize

    (inputFilePaths map sampleSingleFile(sampleRatio) mkString, dataSize)
  }
}
