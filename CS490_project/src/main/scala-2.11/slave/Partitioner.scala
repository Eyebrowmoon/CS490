package slave

import common._

import java.io._
import java.nio.BufferUnderflowException
import java.nio.channels.FileChannel
import java.util.Arrays

import com.typesafe.scalalogging.Logger

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Sorting
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class Partitioner(inputFilePaths: Vector[String], outputDir: String, pivots: Array[Key], slaveNum: Int) {

  println(inputFilePaths.mkString)

  val logger = Logger("Partitioner")

  val numPartition: Int = pivots.length + 1
  val numEntriesPerChunk: Int = 1024 * 1024   // 100 MB
  val pivotsWithBoundary: Array[Key] = Array.concat(Array(MIN_KEY), pivots, Array(MAX_KEY))

  val pivotEntries =  pivotsWithBoundary map newEntryFromKey

  val queueOrdering = new Ordering[(Entry, EntryReader)] {
    def compare(x: (Entry, EntryReader), y: (Entry, EntryReader)): Int = {
      entryOrdering.compare(x._1, y._1)
    }
  }

  def newEntryFromKey(key: Key): Entry = {
    val entry = new Array[Byte](entryLength)
    Array.copy(key, 0, entry, 0, keyLength)
    entry
  }

  def savePartitions(chunkEntries: Array[Entry], chunkFileName: String): Vector[String] = {
    val pivotIndex = pivotEntries map { pivotEntry =>
      Arrays.binarySearch(chunkEntries, pivotEntry, entryOrdering)
    } map { idx => if (idx < 0) -(idx + 1) else idx }

    (0 until numPartition) map { partitionNum: Int => {
      val fileName = s"${outputDir}/${chunkFileName}_${partitionNum}"
      val entries = chunkEntries.slice(pivotIndex(partitionNum), pivotIndex(partitionNum + 1))

      FileHandler.writeFile(fileName) { entries foreach _.write }

      fileName
    }} toVector
  }

  def addNewEntryToQueue(queue: mutable.PriorityQueue[(Entry, EntryReader)])(entryReader: EntryReader): Unit = {
    val entry = entryReader.readEntry()
    queue.enqueue((entry, entryReader))
  }

  def tryAddNewEntryToQueue(queue: mutable.PriorityQueue[(Entry, EntryReader)])(entryReader: EntryReader): Unit = {
    try { addNewEntryToQueue(queue)(entryReader) }
    catch {
      case e: IOException =>
      case e: Exception => e.printStackTrace()
    }
  }

  @tailrec
  private def mergeLoop(queue: mutable.PriorityQueue[(Entry, EntryReader)], out: BufferedOutputStream): Unit = {
    if (queue.nonEmpty) {
      val (entry, entryReader) = queue.dequeue()

      out.write(entry)
      tryAddNewEntryToQueue(queue)(entryReader)

      mergeLoop(queue, out)
    }
  }

  private def mergeSinglePartitionChunks (fileName: String)(files: Vector[String]): Unit = {
    logger.info(s"Merge to make $fileName")

    val queue = mutable.PriorityQueue[(Entry, EntryReader)]()(queueOrdering.reverse)
    new MultipleRandomAccessFileHandler(files, "r").execute { (rafs, cins) =>
      val entryReaders = rafs.indices map { idx => new EntryReader(rafs(idx), cins(idx))}

      entryReaders foreach tryAddNewEntryToQueue(queue)
      FileHandler.writeFile(fileName) { out => mergeLoop(queue, out) }
    }
  }

  private def mergeChunksGroupedByPartition(chunks: Vector[Vector[String]], fileIndex: Int): Vector[String] = {
    chunks.indices map { partitionNum =>
      val fileName = s"${outputDir}/partition_${fileIndex}_${partitionNum}_${slaveNum}"
      val files = chunks(partitionNum)

      mergeSinglePartitionChunks(fileName)(chunks(partitionNum))
      files foreach {file => new File(file).delete()}

      fileName
    } toVector
  }

  private def partitionSingleChunk(path: String, fileIndex: Int)(chunkIndex: Int): Vector[String] = {
    logger.info(s"Partition $path - chunk$chunkIndex")

    val chunkEntries = new RandomAccessFileHandler(path, "r").execute { case (raf, cin) =>
      val entryReader = new EntryReader(raf, cin)
      entryReader.readEntriesFrom(chunkIndex.toLong * numEntriesPerChunk * entryLength, numEntriesPerChunk)
    } toArray

    Sorting.quickSort(chunkEntries)(entryOrdering)
    savePartitions(chunkEntries, s"chunk_${fileIndex}_${chunkIndex}")
  }

  private def partitionSingleFile(path: String): Vector[String] = {
    logger.info(s"Partition $path")

    val fileSize = FileHandler.getFileSize(path)
    val numChunk: Int = Math.ceil(fileSize * 1.0 / numEntriesPerChunk / entryLength).toInt
    val fileIndex = inputFilePaths indexOf path

    val chunkedFiles = (0 until numChunk) map partitionSingleChunk(path, fileIndex) toVector
    val chunksGroupedByPartition: Vector[Vector[String]] = chunkedFiles.transpose

    mergeChunksGroupedByPartition(chunksGroupedByPartition, fileIndex)
  }

  def partitionFiles(): Future[Vector[Vector[String]]] = Future {
    logger.info("Partition start")
    val partitionedFiles = inputFilePaths.map(partitionSingleFile)
    logger.info("Partition end")

    partitionedFiles.transpose
  }
}
