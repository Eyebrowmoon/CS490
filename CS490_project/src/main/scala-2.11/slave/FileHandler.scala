package slave

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import scala.collection.mutable
import com.typesafe.scalalogging.Logger
import common._

class RandomAccessFileHandler(path: String, flag: String) {

  val logger = Logger(s"RandomAccessFileReader - $path")

  protected var rafOption: Option[RandomAccessFile] = None
  protected var channelOption: Option[FileChannel] = None

  def init(): (RandomAccessFile, FileChannel) = {
    val raf = new RandomAccessFile(path, flag)
    rafOption = Some(raf)

    val cin = raf.getChannel
    channelOption = Some(cin)

    (raf, cin)
  }

  def close(): Unit = {
    rafOption foreach { _.close() }
    channelOption foreach { _.close() }
  }

  /*
   * execute method provides safe way to handle the Random Access File
   * */
  def execute[T](handler: (RandomAccessFile, FileChannel) => T): T = {
    try {
      val (raf, channel) = init()
      handler(raf, channel)
    } finally {
      close()
    }
  }

  def executeWithManualClose[T](handler: (RandomAccessFile, FileChannel) => T): T = {
    val (raf, channel) = init()
    handler(raf, channel)
  }
}

class MultipleRandomAccessFileHandler(paths: Vector[String], flag: String) {

  val logger = Logger(s"MultipleRandomAccessFileReader")

  private val openedRafs: mutable.Set[RandomAccessFile] = mutable.Set.empty[RandomAccessFile]
  private val openedChannels: mutable.Set[FileChannel] = mutable.Set.empty[FileChannel]

  private def openRaf(path: String): RandomAccessFile = {
    val raf = new RandomAccessFile(path, flag)
    openedRafs.add(raf)
    raf
  }

  private def getChannel(raf: RandomAccessFile): FileChannel = {
    val channel = raf.getChannel
    openedChannels.add(channel)
    channel
  }

  private def init(): (Vector[RandomAccessFile], Vector[FileChannel]) = {
    val rafs = paths.map(openRaf)
    val channels = rafs.map(getChannel)

    (rafs, channels)
  }

  private def close(): Unit = {
    openedRafs foreach { _.close() }
    openedChannels foreach { _.close() }
  }

  /*
   * execute method provides safe way to handle the Random Access Files
   * But, if you want to keep the file open, you must call init and close methods explicitly.
   * */
  def execute[T](handler: (Vector[RandomAccessFile], Vector[FileChannel]) => T): T = {
    try {
      val (rafs, channels) = init()
      handler(rafs, channels)
    } finally {
      close()
    }
  }
}

class EntryReader(raf: RandomAccessFile, cin: FileChannel) {

  val logger = Logger(s"EntryReader")
  val entryBuffer = ByteBuffer.allocateDirect(entryLength)

  private def readToEntryBuffer(): Int = {
    entryBuffer.clear()
    val readLength = cin.read(entryBuffer)
    entryBuffer.flip()

    readLength
  }

  def readEntry(): Entry = {
    val readLength = readToEntryBuffer()
    if (readLength == entryLength) {
      val entry = new Array[Byte](entryLength)
      entryBuffer.get(entry)
      entry
    } else { throw new IOException() }
  }

  def readEntryFrom(pos: Long): Entry = {
    raf.seek(pos)
    readEntry()
  }

  def readEntriesFrom(pos: Long, numEntries: Int): Vector[Entry] = {
    val remainedEntries = (raf.length - pos) / entryLength
    if (remainedEntries < 0) Vector.empty[Entry]
    else {
      val numEntriesToRead = List(numEntries, remainedEntries.toInt).min

      raf.seek(pos)
      (0 until numEntriesToRead) map { _ => readEntry() } toVector
    }
  }
}

object FileHandler {

  val logger = Logger("FileHandler")

  def getPathsFromDir(dirName: String): Vector[String] = {
    val dir = new File(dirName)
    if (dir.exists && dir.isDirectory) dir.listFiles.filter(_.isFile).map{_.getCanonicalPath}.toVector
    else Vector[String]()
  }

  def getFileSize(path: String): Long = {
    val file = new File(path)
    if (file.exists) file.length
    else 0
  }

  def readFile(path: String)(handler: BufferedInputStream => Unit): Unit = {
    var inOption: Option[BufferedInputStream] = None

    try {
      val in = new BufferedInputStream(new FileInputStream(path))
      inOption = Some(in)

      handler(in)
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      inOption.foreach{ _.close() }
    }
  }

  def writeFile(path: String)(handler: BufferedOutputStream => Unit): Unit = {
    var outOption: Option[BufferedOutputStream] = None

    try {
      val out = new BufferedOutputStream(new FileOutputStream(path))
      outOption = Some(out)

      handler(out)
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      outOption foreach { _.close() }
    }
  }

  def readToBuffer(cin: FileChannel, buffer: ByteBuffer): Int = {
    buffer.clear()
    val readLength = cin.read(buffer)
    buffer.flip()

    readLength
  }

  def readByteArray(cin: FileChannel, buffer: ByteBuffer): Array[Byte] = {
    val readLength = readToBuffer(cin, buffer)
    val entry = new Array[Byte](readLength)
    buffer.get(entry)
    entry
  }
}
