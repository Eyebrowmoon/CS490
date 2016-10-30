package Common

import java.util.concurrent.LinkedBlockingQueue

abstract class StateManager extends Runnable {
  protected val messageQueue: LinkedBlockingQueue[Message] = new LinkedBlockingQueue[Message]()

  def interrupted: Boolean = Thread.currentThread.isInterrupted

  /* No synchronized since BlockingQueue is thread safe */
  def addMessage(message: Message): Unit = messageQueue.add(message)

  override def run(): Unit = {
    try {
      while (!interrupted) {
        val msg = messageQueue.take()
        handleMessage(msg)
      }
    } catch {
      case e: InterruptedException =>
      case e: Exception => e.printStackTrace()
    }
  }

  protected def handleMessage(message: Message): Unit
}
