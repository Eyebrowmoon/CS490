package slave

import common._

import java.io._
import java.util.Arrays

import com.typesafe.scalalogging.Logger

import scala.util.Sorting
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class Partitioner(inputFilePaths: Vector[String], outputDir: String, pivots: Array[Key], slaveNum: Int) {

  val logger = Logger("Partitioner")

  val numPartition: Int = pivots.length + 1
  val numEntriesPerChunk: Int = 1024 * 1024   // 100 MB
  val pivotsWithBoundary: Array[Key] = Array.concat(Array(MIN_KEY), pivots, Array(MAX_KEY))

  val pivotEntries =  pivotsWithBoundary map newEntryFromKey

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

  private def mergeChunksGroupedByPartition(chunks: Vector[Vector[String]], fileIndex: Int): Vector[String] = {
    chunks.indices map { partitionNum =>
      val fileName = s"${outputDir}/partition_${fileIndex}_${partitionNum}_${slaveNum}"
      val files = chunks(partitionNum)

      Merger(files, fileName)
      files foreach { file => new File(file).delete() }

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

    val mergedPartitions = mergeChunksGroupedByPartition(chunksGroupedByPartition, fileIndex)
    mergedPartitions.filter(new File(_).length == 0).foreach(new File(_).delete())

    mergedPartitions.filter(new File(_).length > 0)
  }

  def partitionFiles(): Future[Vector[Vector[String]]] = Future {
    logger.info("Partition start")
    val partitionedFiles = inputFilePaths.map(partitionSingleFile)
    logger.info("Partition end")

    partitionedFiles.transpose
  }
}
