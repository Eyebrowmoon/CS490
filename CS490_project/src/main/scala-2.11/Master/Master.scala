package master

object Master {
  def main(args: Array[String]): Unit = {
    try{
      val numSlave: Int = args(0).toInt
      (new MasterStateManager(numSlave)).run()
    } catch {
      case e: IllegalArgumentException => printUsage()
      case e: Exception => e.printStackTrace()
    }
  }

  def printUsage(): Unit = println("Usage: master <# of slaves>")
}
