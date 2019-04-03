package uk.gov.ons.mdr.io.sequence

import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.io.{BytesWritable, Text}
import org.slf4j.{Logger, LoggerFactory}

trait SequenceWriter extends java.io.Closeable {
  def write(fileData: FileData): Unit
}

class RollingSequenceWriter(sequenceOutputDir: SequenceOutputDir,
                            zipInputFile: ZipInputFile,
                            byteThreshold: Long) extends SequenceWriter {

  this: WriterFactory =>

  def logger: Logger = LoggerFactory.getLogger(this.getClass)

  private var currentFileIndex = 1
  private var bytesWritten = 0L

  def currentFileName(): String = f"${zipInputFile.file.getName.stripSuffix(".zip")}_$currentFileIndex%02d.seq"

  private var writer = createWriter(currentFileName())

  private def roll(): Writer = {

    currentFileIndex += 1
    writer.close()
    bytesWritten = 0L

    logger.info(s"Rolling file, new output ${currentFileName()}")
    createWriter(sequenceOutputDir.file.getPath + "/" + currentFileName())
  }

  override def write(fileData: FileData): Unit = {
    this synchronized {

      if (bytesWritten > byteThreshold) {
        writer = roll()
      }

      writer.append(new Text(fileData.name), new BytesWritable(fileData.content))

      val nameByteLength = fileData.name.getBytes.length
      val contentByteLength = fileData.content.length

      bytesWritten += nameByteLength + contentByteLength
    }
  }

  override def close(): Unit = {
    writer.close()
  }
}
