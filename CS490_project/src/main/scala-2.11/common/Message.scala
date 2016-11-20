package common

abstract class Message
case class PivotMessage(pivots: List[Key]) extends Message
case class SampleMessage(address:String, numData: Long, sample: String) extends Message
case class SlaveInfoMessage(slaveIP: Array[String], pivots: String) extends Message
case class PartitionDoneMessage(partitions: Vector[Vector[String]]) extends Message
case object SlaveFullMessage extends Message
case object DoneMessage extends Message