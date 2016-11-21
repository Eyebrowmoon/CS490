package master

import common._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object PivotCalculator {
  def getPivots(samples: List[Key], numSlave: Int): Future[List[Key]] = Future {
    val numPartition: Int = numSlave * numPartitionForSlave
    val pivotInterval: Int = samples.size / numPartition
    val sortedSamples: List[Key] = samples.sorted(keyOrdering)

    (1 until numPartition) map { i => sortedSamples(pivotInterval * i) } toList
  }
}
