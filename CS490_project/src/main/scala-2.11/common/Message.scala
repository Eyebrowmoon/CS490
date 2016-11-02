package common

import java.nio.channels.SocketChannel

abstract class Message
case class ConnectionMessage(socket: SocketChannel) extends Message
case class SampleMessage(numData: Long, keys: Array[Key], handler: SocketHandler) extends Message
case class PivotMessage(pivots: List[Key]) extends Message
case class DoneMessage(handler: SocketHandler) extends Message

abstract class SendableMessage extends Message
case class SendableSampleMessage(numData: Long, sampleSize: Int) extends SendableMessage
case class SlaveInfoMessage(slaveIP: Array[String], pivots: String, slaveNum: Int) extends SendableMessage
case object SendableDoneMessage extends SendableMessage
case object AckMessage extends SendableMessage