package uk.gov.ons.mdr.io.sequence

import java.nio.file.Paths

import scopt.OptionParser

/** The main entry point for the sequencer application.
  *
  * Parses command line arguments, then calls [[uk.gov.ons.mdr.io.sequence.ZipToSequence]].
  *
  * @example
  * `java -jar sequencer.jar some_archive.zip`
  */
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

  private val parser: OptionParser[Config] = new scopt.OptionParser[Config](programName = "java -jar sequencer.jar") {

    val defaultConfig = Config()

    val helpText = """Command line utility to convert zip archives into Hadoop Sequence files."""

    head(xs = s"sequencer\n\n$helpText\n")

    implicit val sequenceOutputDirRead: scopt.Read[SequenceOutputDir] =
      scopt.Read.reads[SequenceOutputDir](path => SequenceOutputDir(Paths.get(path)))

    opt[SequenceOutputDir]('t', name = "target")
      .valueName("<dir>")
      .action((x, c) => c.copy(targetDir = x))
      .text(s"Target directory for sequence file creation, default: ${defaultConfig.targetDir.path}")

    opt[Unit]('s', "singleFileMode")
      .action( (_, c) => c.copy(singleFileMode = true) )
      .text("Flag to produce a single sequence file (switches off batch mode)")

    opt[String]('b', name = "batchSize")
        .valueName("<string>")
        .action((x, c) => c.copy(targetBytes = parseMemoryString(x)))
        .text("Approximate sequence batch size (e.g. 200m, 1g)")

    implicit val zipInputFileRead: scopt.Read[ZipInputFile] =
      scopt.Read.reads[ZipInputFile](path => ZipInputFile(Paths.get(path)))

    arg[ZipInputFile]("<zip_file>")
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
            ZipToSequence.zipToSingleSequence(config)
          } else {
            ZipToSequence.zipToRollingSequence(config)
          }

        case None =>
          parser.reportError(msg = "No options set")
      }
    }
  }
}
