package slave

object Slave {
  def main(args:Array[String]): Unit = {
    try {
      val masterAddress = args(0)
      val inputDirs = parseInputDirs(args)
      val outputDirs = parseOutputDir(args)

      (new SlaveStateManager(masterAddress, inputDirs, outputDirs)).run()
    } catch {
      case e: IllegalArgumentException => printUsage()
      case e: Exception => e.printStackTrace()
    }
  }

  def parseInputDirs(args: Array[String]): Array[String] = {
    val inputOption: Int = args.indexOf("-I")
    val outputOption: Int = args.indexOf("-O")

    if (inputOption < 0 || outputOption < 0) throw new IllegalArgumentException
    else {
      val inputEnd = if (inputOption < outputOption) outputOption else args.length
      val inputDirectories = args.slice(inputOption + 1, inputEnd)

      if (inputDirectories.isEmpty) throw new IllegalArgumentException
      inputDirectories
    }
  }

  def parseOutputDir(args: Array[String]): String = {
    val outputOption: Int = args.indexOf("-O")
    args(outputOption + 1)
  }

  def printUsage(): Unit = {
    println("slave <master IP:Port> -I" +
      " <input directory> <input directory> ... <input directory>" +
      " -O <output directory>")
  }
}
