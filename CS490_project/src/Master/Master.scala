package Master

object Master {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) printUsage()
    else {
      val numSlaveOption: Option[Int] = toInt(args(1))

      numSlaveOption match {
        case Some(numSlave) => (new MasterStateManager(numSlave)).run()
        case None => printUsage()
      }
    }
  }

  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  def printUsage(): Unit = println("Usage: master numSlave")
}
