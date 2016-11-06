package common

import java.io.{ByteArrayInputStream}
import java.util

import io.netty.channel.{ChannelHandlerContext}
import io.netty.handler.codec.{MessageToMessageDecoder, MessageToMessageEncoder}
import io.netty.handler.stream.ChunkedStream

import scala.pickling.Defaults._
import scala.pickling.PicklingException
import scala.pickling.json._

class MessageToStringEncoder extends MessageToMessageEncoder[Message] {

  override def encode(ctx: ChannelHandlerContext, msg: Message, out: util.List[AnyRef]): Unit = {
    val pickledMsg: String = msg.pickle.value

    val inputStream = new ByteArrayInputStream(pickledMsg.getBytes())

    println("Sent:")
    if (pickledMsg.length > 500) {
      println(pickledMsg.substring(0, 500))
      println("...\n")
    } else println(pickledMsg)

    out.add(new ChunkedStream(inputStream))
  }
}

class StringToMessageDecoder extends MessageToMessageDecoder[String] {

  var messageAccumulated: String = ""

  override def decode(ctx: ChannelHandlerContext, msg: String, out: util.List[AnyRef]): Unit = {
    try {
      messageAccumulated += msg
      val message = messageAccumulated.unpickle[Message]

      println("Received:")
      if (messageAccumulated.length > 500) {
        println(messageAccumulated.substring(0, 500))
        println("...")
      } else println(messageAccumulated)
      println("")

      out.add(message)
      messageAccumulated = ""
    } catch {
      case _: PicklingException =>
    }
  }
}
