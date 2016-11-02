package master

import common._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object PivotCalculator {
  def getPivots(samples: List[Key], numSlave: Int): Future[List[Key]] = Future {
    val pivotInterval: Int = samples.size / numSlave
    val sortedSamples: List[Key] = samples.sorted

    (0 until (numSlave - 1)) map { i => sortedSamples(pivotInterval * i) } toList
  }
}
