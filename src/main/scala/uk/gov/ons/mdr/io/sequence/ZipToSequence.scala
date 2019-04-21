package uk.gov.ons.mdr.io.sequence

import org.slf4j.{Logger, LoggerFactory}

object ZipToSequence {

  def logger: Logger = LoggerFactory.getLogger(this.getClass)

  def zipToSingleSequence(config: Config): Unit = {

    logger.info("Creating a single sequence file")

    zipToSequence(config, fetchSingleWriter)
  }

  def zipToRollingSequence(config: Config): Unit = {

    logger.info("Creating multiple sequence files")

    zipToSequence(config, fetchRollingWriter)

  }

  private def fetchSingleWriter(config: Config): SequenceWriter = {

    val seqFileNamePrefix = getPrefix(config.sourceFile)

    new SingleSequenceWriter(
      config.targetDir, seqFileNamePrefix) with SequenceFileWriterFactory
  }

  private def fetchRollingWriter(config: Config): SequenceWriter = {

    val seqFileNamePrefix = getPrefix(config.sourceFile)

    new RollingSequenceWriter(
      config.targetDir, seqFileNamePrefix, config.targetBytes) with SequenceFileWriterFactory
  }

  private def zipToSequence(config: Config, fetcher: Config => SequenceWriter): Unit = {

    withZipReader(config) {

      zipReader =>

        withSequenceWriter(config, fetcher) {

          sequenceWriter => convert(zipReader, sequenceWriter)
        }
    }
  }

  private def getPrefix(zipInputFile: ZipInputFile): String = {
    zipInputFile.path.getFileName.toString.stripSuffix(".zip")
  }

  private def createZipReader(config: Config): ZipReader = {
    new ZipReader(config.sourceFile.path)
  }

  private def withZipReader(config: Config)(op: ZipReader => Unit): Unit = {

    val zipReader = new ZipReader(config.sourceFile.path)

    try {
      op(zipReader)
    } finally {
      zipReader.close()
    }
  }

  private def withSequenceWriter(config: Config, fetcher: Config => SequenceWriter)
                                (op: SequenceWriter => Unit): Unit = {

    val sequenceWriter = fetcher(config)

    try {
      op(sequenceWriter)
    } finally {
      sequenceWriter.close()
    }
  }

  private def convert(zipReader: ZipReader, writer: SequenceWriter): Unit = {
    zipReader.iterator().foreach(writer.write)
  }
}