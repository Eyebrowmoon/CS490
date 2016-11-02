package master

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import PivotCalculator._
import common._

import scala.concurrent.duration._
import scala.concurrent.Await

@RunWith(classOf[JUnitRunner])
class PivotSuite extends FunSuite {
  val sampleKeySet: List[Key] = (0 to 0xff) map { i =>
    List.fill[Byte](keyLength)(i.toByte).toArray
  } toList

  test("getPivot calculates correct pivot for sampleKeySet") {
    val pivotFuture = getPivots(sampleKeySet, 2)
    val pivots: List[Key] = Await.result(pivotFuture, 5 seconds)

    val numPartition = numPartitionForSlave * 2
    val pivotInterval = 0x100 / numPartition

    assert(pivots.length == (numPartition - 1))
    pivots.indices foreach { i =>
      assert(pivots(i) == sampleKeySet((i + 1) * pivotInterval))
    }
  }

  test("getPivot calculates correct pivot for sampleKeySet reverse") {
    val pivotFuture = getPivots(sampleKeySet.reverse, 2)
    val pivots: List[Key] = Await.result(pivotFuture, 5 seconds)

    val numPartition = numPartitionForSlave * 2
    val pivotInterval = 0x100 / numPartition

    assert(pivots.length == (numPartition - 1))
    pivots.indices foreach { i =>
      assert(pivots(i) == sampleKeySet((i + 1) * pivotInterval))
    }
  }
}
