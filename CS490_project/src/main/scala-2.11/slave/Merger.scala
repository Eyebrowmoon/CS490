package slave

import java.io.{BufferedOutputStream, IOException}

import common._

import scala.annotation.tailrec
import scala.collection.mutable

import com.typesafe.scalalogging.Logger

class Merger(files: Vector[String], fileName: String) {

  import Merger.queueOrdering

  val logger = Logger(s"Merger - $fileName")

  val queue = mutable.PriorityQueue[(Entry, EntryReader)]()(queueOrdering.reverse)

  private def addNewEntryToQueue(entryReader: EntryReader): Unit = {
    val entry = entryReader.readEntry()
    queue.enqueue((entry, entryReader))
  }

  private def tryAddNewEntryToQueue(entryReader: EntryReader): Unit = {
    try { addNewEntryToQueue(entryReader) }
    catch {
      case e: IOException =>
      case e: Exception => e.printStackTrace()
    }
  }

  @tailrec
  private def mergeLoop(out: BufferedOutputStream): Unit = {
    if (queue.nonEmpty) {
      val (entry, entryReader) = queue.dequeue()

      out.write(entry)
      tryAddNewEntryToQueue(entryReader)

      mergeLoop(out)
    }
  }

  def merge(): Unit = {
    logger.info(s"Merge to make $fileName")

    new MultipleRandomAccessFileHandler(files, "r").execute { (rafs, cins) =>
      val entryReaders = rafs.indices map { idx => new EntryReader(rafs(idx), cins(idx))}

      entryReaders foreach tryAddNewEntryToQueue
      FileHandler.writeFile(fileName)(mergeLoop)
    }
  }
}

object Merger {
  val queueOrdering = new Ordering[(Entry, EntryReader)] {
    def compare(x: (Entry, EntryReader), y: (Entry, EntryReader)): Int = {
      entryOrdering.compare(x._1, y._1)
    }
  }

  def apply(files: Vector[String], fileName: String): Unit = (new Merger(files, fileName)).merge()
}
