package uk.gov.ons.mdr.io.sequence

import scopt.OptionParser

object Sequencer {

  private def parseMemoryString(memoryString: String): Long = {

      val GigabytePattern = "([0-9\\.]+)g".r
      val MegabytePattern = "([0-9\\.]+)m".r
      val KilobytePattern = "([0-9\\.]+)k".r
      val BytePattern = "([0-9\\.]+)b".r

      memoryString match {
        case GigabytePattern(digits) => (1000000000L * digits.toDouble).toLong
        case MegabytePattern(digits) => (1000000L * digits.toDouble).toLong
        case KilobytePattern(digits) => (1000L * digits.toDouble).toLong
        case BytePattern(digits) => digits.toLong
      }
    }

  private val parser: OptionParser[Config] = new scopt.OptionParser[Config](programName = "sequencer") {

    val defaultConfig = Config()

    val helpText = """Command line utility to convert zip archives into Hadoop Sequence files."""

    head(xs = s"sequencer\n\n$helpText\n")

    opt[String]('t', name = "target")
      .valueName("<dir>")
      .action((x, c) => c.copy(targetDir = x))
      .text(s"Target directory for sequence file creation, default: ${defaultConfig.targetDir}")

    opt[Unit]('s', "singleFileMode")
      .action( (_, c) => c.copy(singleFileMode = true) )
      .text("Flag to run in singleFileMode")

    opt[String]('b', name = "batchSize")
        .valueName("<string>")
        .action((x, c) => c.copy(targetBytes = parseMemoryString(x)))
        .text("When not in singleFileMode controls the batch size for the sequence file (e.g. 200m, 1g)")

    arg[String]("<zip_file>")
        .action((x, c) => c.copy(sourceFile = x))
        .text("Zip archive for conversion")

    help(name = "help").text("Show help text")
  }

  def main(args: Array[String]): Unit = {

    if (args.isEmpty) {
      println("No arguments passed.\n")
      parser.parse(args = Array("--help"), init = Config())
    } else {
      parser.parse(args, init = Config()) match {
        case Some(config) =>

          if (config.singleFileMode) {
            println("Creating a single sequence file")
            ZipToSequence.zipToSingleSequence(config)
          } else {
            println("Creating multiple sequence files")
            ZipToSequence.zipToRollingSequence(config)
          }

        case None =>
          parser.reportError(msg = "No options set")
      }
    }
  }
}