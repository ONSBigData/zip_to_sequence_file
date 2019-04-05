package uk.gov.ons.mdr.io.sequence


import java.nio.file.Paths

import org.apache.hadoop.io.{BytesWritable, Text}
import org.slf4j.{Logger, LoggerFactory}

/** Writes to a single sequence file */
class SingleSequenceWriter(sequenceOutputDir: SequenceOutputDir, seqFileNamePrefix: String) extends SequenceWriter {

  this: WriterFactory =>

  def logger: Logger = LoggerFactory.getLogger(this.getClass)

  private[this] val dirString = sequenceOutputDir.path.toString
  private[this] val outFileName = s"$seqFileNamePrefix.seq"

  logger.info(s"Output file: $outFileName")

  val outPath = Paths.get(dirString, outFileName)

  private[this] var writer = createWriter(outPath)

  override def write(fileData: FileData): Unit = {
    writer.append(new Text(fileData.name), new BytesWritable(fileData.content))
  }

  override def close(): Unit = {
    writer.close()
  }
}
