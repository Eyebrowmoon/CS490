package Common

import java.net.Socket

abstract class Message {}

case class ConnectionMessage(socket: Socket) extends Message {}