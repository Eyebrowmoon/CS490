package Master

object Master {
  def main(args: Array[String]): Unit = {
    try{
      val numSlave: Int = args(1).toInt

      (new MasterStateManager(numSlave)).run()
    } catch {
      case e: Exception => printUsage()
    }
  }

  def printUsage(): Unit = println("Usage: master <# of slaves>")
}
