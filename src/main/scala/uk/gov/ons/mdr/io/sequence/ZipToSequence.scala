package uk.gov.ons.mdr.io.sequence

import org.slf4j.{Logger, LoggerFactory}

object ZipToSequence {

  def logger: Logger = LoggerFactory.getLogger(this.getClass)

  def zipToSingleSequence(config: Config): Unit = {

    logger.info("Creating a single sequence file")

    var zipReader = createZipReader(config)

    var sequenceWriter = new SingleSequenceWriter(config.targetDir, config.sourceFile)
      with SequenceFileWriterFactory

    convert(zipReader, sequenceWriter)
  }

  def zipToRollingSequence(config: Config): Unit = {

    logger.info("Creating multiple sequence files")

    var zipReader = createZipReader(config)

    var rollingSequenceWriter = new RollingSequenceWriter(
      config.targetDir, config.sourceFile, config.targetBytes) with SequenceFileWriterFactory

    convert(zipReader, rollingSequenceWriter)
  }

  private def createZipReader(config: Config): ZipReader = {
    new ZipReader(config.sourceFile.file.getPath)
  }

  private def convert(zipReader: ZipReader, writer: SequenceWriter): Unit = {
    try {
      zipReader.iterator().foreach(writer.write)
    } finally {
      writer.close()
    }
  }
}
