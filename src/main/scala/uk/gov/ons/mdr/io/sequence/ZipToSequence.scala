package uk.gov.ons.mdr.io.sequence

object ZipToSequence {

  def zipToSingleSequence(config: Config): Unit = {

    var zipReader = createZipReader(config)

    var sequenceWriter = new SingleSequenceWriter(config.targetDir, config.sourceFile)
      with SequenceFileWriterFactory

    convert(zipReader, sequenceWriter)
  }

  def zipToRollingSequence(config: Config): Unit = {

    var zipReader = createZipReader(config)

    var rollingSequenceWriter = new RollingSequenceWriter(
      config.sourceFile.stripSuffix(".zip"),
      config.targetBytes) with SequenceFileWriterFactory

    convert(zipReader, rollingSequenceWriter)
  }

  private def createZipReader(config: Config): ZipReader = {
    new ZipReader(config.sourceFile)
  }

  private def convert(zipReader: ZipReader, writer: SequenceWriter): Unit = {
    try {
      zipReader.iterator().foreach(writer.write)
    } finally {
      writer.close()
    }
  }
}
