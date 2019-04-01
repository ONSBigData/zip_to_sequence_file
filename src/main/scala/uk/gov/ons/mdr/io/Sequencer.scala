package uk.gov.ons.mdr.io


object Sequencer {

  case class Config(sourceFile: String = "", targetDir: String = ".")

  private val parser = new scopt.OptionParser[Config](programName = "sequencer") {

    val defaultConfig = Config()

    val helpText = """Commandline utility to convert zip archives into Hadoop Sequence files."""

    head(xs = s"sequencer\n\n$helpText\n")

    opt[String]('s', name = "source")
      .valueName("<file>")
      .action((x, c) => c.copy(sourceFile = x))
      .text("Zip archive for conversion")

    opt[String]('t', name = "target")
      .valueName("<file>")
      .action((x, c) => c.copy(targetDir = x))
      .text(s"Target directory for sequence file creation, default: ${defaultConfig.targetDir}")

    help(name = "help").text("Show help text")
  }

  def main(args: Array[String]): Unit = {

    if (args.isEmpty) {
      println("No arguments passed.\n")
      parser.parse(args = Array("--help"), init = Config())
    } else {
      parser.parse(args, init = Config()) match {
        case Some(config) =>
          println("Config received")

          println(s"sourceFile ${config.sourceFile}")
          println(s"targetDir ${config.targetDir}")

        case None =>
          parser.reportError(msg = "No options set")
      }
    }
  }
}