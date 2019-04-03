package uk.gov.ons.mdr.io.sequence

import java.nio.file.Path

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.hadoop.io.{BytesWritable, SequenceFile, Text}

trait WriterFactory {
  def createWriter(path: Path): Writer
}

trait SequenceFileWriterFactory extends WriterFactory {

  def createWriter(path: Path): Writer = {

    val conf = new Configuration()
    fs.FileSystem.getLocal(conf)

    val uri = path.toUri
    val hadoopPath = new fs.Path(uri)

    SequenceFile.createWriter(conf,
      Writer.file(hadoopPath),
      Writer.keyClass(classOf[Text]),
      Writer.valueClass(classOf[BytesWritable]),
      Writer.compression(CompressionType.NONE))
  }
}
