package common

import java.net.Socket

abstract class Message
case class ConnectionMessage(socket: Socket) extends Message
case class SampleMessage(numData: Long, keys: Array[Key], handler: SocketHandler) extends Message
case class PivotMessage(pivots: List[Key]) extends Message
case class DoneMessage(handler: SocketHandler) extends Message

abstract class SendableMessage extends Message
case class SendableSampleMessage(numData: Long, sampleSize: Int) extends SendableMessage
case class SlaveInfoMessage(slaveIP: Array[String], pivots: Array[String], slaveNum: Int) extends SendableMessage
case object SendableDoneMessage extends SendableMessage