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

class Partitioner(fileHandler: FileHandler, pivots: Array[Key], slaveNum: Int) {

  val logger = Logger("Partitioner")

  val numPartition: Int = pivots.length + 1
  val numEntriesPerChunk: Int = 1024 * 1024   // 100 MB
  val pivotsWithBoundary: Array[Key] = Array.concat(Array(MIN_KEY), pivots, Array(MAX_KEY))

  val pivotEntries =  pivotsWithBoundary map newEntryFromKey

  val queueOrdering = new Ordering[(Entry, FileChannel)] {
    def compare(x: (Entry, FileChannel), y: (Entry, FileChannel)): Int = {
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
      val fileName = s"${fileHandler.outputDir}/${chunkFileName}_${partitionNum}"
      val entries = chunkEntries.slice(pivotIndex(partitionNum), pivotIndex(partitionNum + 1))

      fileHandler.saveEntriesToFile(entries, fileName)

      fileName
    }} toVector
  }

  private def partitionSingleChunk(file: File, fileIndex: Int)(chunkIndex: Int): Vector[String] = {
    logger.info(s"Partition ${file.getCanonicalPath} - chunk${chunkIndex}")

    val chunkEntries = fileHandler
      .readEntries(file, chunkIndex.toLong * numEntriesPerChunk * entryLength, numEntriesPerChunk)

    Sorting.quickSort(chunkEntries)(entryOrdering)

    savePartitions(chunkEntries, s"chunk_${fileIndex}_${chunkIndex}")
  }

  def addNewEntryToQueue(queue: mutable.PriorityQueue[(Entry, FileChannel)])(cin: FileChannel): Unit = {
    val entry = fileHandler.readEntryFromChannel(cin)
    queue.enqueue((entry, cin))
  }

  def tryAddNewEntryToQueue(queue: mutable.PriorityQueue[(Entry, FileChannel)])(cin: FileChannel): Unit = {
    try { addNewEntryToQueue(queue)(cin) }
    catch {
      case e: BufferUnderflowException =>
      case e: Exception => e.printStackTrace()
    }
  }

  @tailrec
  private def mergeLoop(queue: mutable.PriorityQueue[(Entry, FileChannel)], out: FileOutputStream): Unit = {
    if (queue.nonEmpty) {
      try {
        val (entry, cin) = queue.dequeue()
        out.write(entry)
        addNewEntryToQueue(queue)(cin)
      } catch {
        case e: BufferUnderflowException =>
        case e: Exception => e.printStackTrace()
      }
      mergeLoop(queue, out)
    }
  }

  private def mergeSinglePartitionChunks (fileName: String)(files: Vector[String]): Unit = {
    val rafs = files.map(path => new RandomAccessFile(path, "r"))
    val cins = rafs.map(_.getChannel)
    val out = new FileOutputStream(fileName)
    val queue = mutable.PriorityQueue[(Entry, FileChannel)]()(queueOrdering.reverse)

    logger.info(s"Merge to make $fileName")

    cins foreach tryAddNewEntryToQueue(queue)
    mergeLoop(queue, out)

    rafs foreach {_.close()}
    cins foreach {_.close()}
    out.close()
  }

  private def mergeChunksGroupedByPartition(chunks: Vector[Vector[String]], fileIndex: Int): Vector[String] = {
    chunks.indices map { partitionNum =>
      val fileName = s"${fileHandler.outputDir}/partition_${fileIndex}_${partitionNum}_${slaveNum}"
      val files = chunks(partitionNum)

      mergeSinglePartitionChunks(fileName)(chunks(partitionNum))
      files foreach {file => new File(file).delete()}

      fileName
    } toVector
  }

  private def partitionSingleFile(file: File): Vector[String] = {
    val numChunk: Int = Math.ceil(file.length.toDouble / numEntriesPerChunk / entryLength).toInt
    val fileIndex = fileHandler.inputFileList indexOf file

    logger.info(s"Partition ${file.getCanonicalPath}")

    val chunkedFiles = (0 until numChunk) map partitionSingleChunk(file, fileIndex) toVector
    val chunksGroupedByPartition: Vector[Vector[String]] = chunkedFiles.transpose

    mergeChunksGroupedByPartition(chunksGroupedByPartition, fileIndex)
  }

  def partitionFiles(): Future[Vector[Vector[String]]] = Future {
    logger.info("Partition start")
    val partitionedFiles = fileHandler.inputFileList.map(partitionSingleFile).toVector
    logger.info("Partition end")

    partitionedFiles.transpose
  }
}
