package common

import java.io.ByteArrayInputStream
import java.util

import com.typesafe.scalalogging.Logger
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.{MessageToMessageDecoder, MessageToMessageEncoder}
import io.netty.handler.stream.ChunkedStream

import scala.pickling.Defaults._
import scala.pickling.PicklingException
import scala.pickling.json._

class MessageToStringEncoder extends MessageToMessageEncoder[Message] {

  val logger = Logger("MessageToStringEncoder")

  override def encode(ctx: ChannelHandlerContext, msg: Message, out: util.List[AnyRef]): Unit = {
    val pickledMsg: String = msg.pickle.value

    val inputStream = new ByteArrayInputStream(pickledMsg.getBytes())

    logger.debug("Sent:")
    if (pickledMsg.length > 500) {
      logger.debug(pickledMsg.substring(0, 500))
      logger.debug("...\n")
    } else logger.debug(pickledMsg)

    out.add(new ChunkedStream(inputStream))
  }
}

class StringToMessageDecoder extends MessageToMessageDecoder[String] {

  val logger = Logger("StringToMessageDecoder")

  var messageAccumulated: String = ""

  override def decode(ctx: ChannelHandlerContext, msg: String, out: util.List[AnyRef]): Unit = {
    try {
      messageAccumulated += msg
      val message = messageAccumulated.unpickle[Message]

      logger.debug("Received:")
      if (messageAccumulated.length > 500) {
        logger.debug(messageAccumulated.substring(0, 500))
        logger.debug("...")
      } else logger.debug(messageAccumulated)

      out.add(message)
      messageAccumulated = ""
    } catch {
      case _: PicklingException =>
    }
  }
}
