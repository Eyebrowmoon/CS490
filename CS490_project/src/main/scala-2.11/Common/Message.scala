package common

import java.net.Socket

abstract class Message
case class ConnectionMessage(socket: Socket) extends Message
case class SampleMessage(numData: Long, keys: Array[Key], handler: SocketHandler) extends Message

trait SendableMessage
case class SendableSampleMessage(numData: Long, keys: String) extends SendableMessage