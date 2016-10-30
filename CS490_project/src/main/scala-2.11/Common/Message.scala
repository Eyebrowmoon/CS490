package Common

import java.net.Socket

abstract class Message
case class ConnectionMessage(socket: Socket) extends Message
case class SampleMessage(numData: Int, keys: Array[Key], handler: Option[SocketHandler]) extends Message

trait SendableMessage
case class SendableSampleMessage(numData: Int, keys: String) extends SendableMessage