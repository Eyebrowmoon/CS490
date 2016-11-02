import scala.annotation.tailrec

package object common {
  type Key = Array[Byte]

  val keyLength = 10
  val valueLength = 90
  val entryLength = 100

  val packetSize = 1024

  def byteToUnsigned(x: Byte): Int = if (x < 0) x.toInt + 0x100 else x.toInt

  // For debug
  def stringToHex(str: String): String = {
    def charToHex(c: Char): String = {
      val cStr = byteToUnsigned(c.toByte).toHexString
      cStr.reverse.padTo(2, '0').reverse.toString
    }

    str.toList.map(charToHex).mkString
  }

  def stringToKeyArray(keyString: String): Array[Key] = {
    val arrayLength = keyString.length / keyLength

    (0 until arrayLength) map { i =>
      keyString.substring(i * keyLength, (i+1) * keyLength)
        .toCharArray
        .map { _.toByte }
    } toArray
  }

  implicit val keyOrdering = new Ordering[Key] {

    def compare(x: Key, y: Key): Int = compareIdx(x, y, 0)

    @tailrec
    def compareIdx(x: Key, y: Key, i: Int): Int = {
      if (i >= keyLength) 0
      else {
        val xUnsigned: Int = byteToUnsigned(x(i))
        val yUnsigned: Int = byteToUnsigned(y(i))

        if (xUnsigned < yUnsigned) -1
        else if (xUnsigned > yUnsigned) 1
        else compareIdx(x, y, i+1)
      }
    }
  }
}
