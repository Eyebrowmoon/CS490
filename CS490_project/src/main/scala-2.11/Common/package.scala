import scala.annotation.tailrec

package object common {
  type Key = Array[Byte]

  val keyLength = 10
  val valueLength = 90
  val entryLength = 100

  val packetSize = 1024

  // For debug
  def string2hex(str: String): String = {
    str.toList.map(_.toInt.toHexString).mkString
  }

  implicit val keyOrdering = new Ordering[Key] {

    def compare(x: Key, y: Key): Int = compareIdx(x, y, 0)

    @tailrec
    def compareIdx(x: Key, y: Key, i: Int): Int = {
      if (i >= keyLength) 0
      else if (x(i) < y(i)) 1
      else if (x(i) > y(i)) -1
      else compareIdx(x, y, i+1)
    }
  }
}
