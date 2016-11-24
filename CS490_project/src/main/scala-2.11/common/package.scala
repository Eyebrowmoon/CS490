import scala.annotation.tailrec

package object common {

  /* Common types */

  type Key = Array[Byte]
  type Entry = Array[Byte]

  /* Common constants */

  val keyLength = 10
  val valueLength = 90
  val entryLength = 100

  val packetSize = 1024

  val numPartitionForSlave: Int = 8

  val MIN_KEY: Key = Array.fill[Byte](keyLength)(0.toByte)
  val MAX_KEY: Key = Array.fill[Byte](keyLength)(0xff.toByte)

  /* Common functions */

  def byteToUnsigned(x: Byte): Int = if (x < 0) x.toInt + 0x100 else x.toInt

  def charToHex(c: Char): String = {
    val cStr = byteToUnsigned(c.toByte).toHexString
    cStr.reverse.padTo(2, '0').reverse.toString
  }

  def stringToHex(str: String): String = {
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

  def keyToString(key: Key): String = key.map(_.toChar).mkString

  /* Orderings */

  @tailrec
  private def compareKey(x: Array[Byte], y: Array[Byte], i: Int): Int = {
    if (i >= keyLength) 0
    else {
      val xUnsigned: Int = byteToUnsigned(x(i))
      val yUnsigned: Int = byteToUnsigned(y(i))

      if (xUnsigned < yUnsigned) -1
      else if (xUnsigned > yUnsigned) 1
      else compareKey(x, y, i+1)
    }
  }

  val keyOrdering = new Ordering[Key] {
    def compare(x: Key, y: Key): Int = compareKey(x, y, 0)
  }

  val entryOrdering = new Ordering[Entry] {
    def compare(x: Entry, y: Entry): Int = compareKey(x, y, 0)
  }
}
