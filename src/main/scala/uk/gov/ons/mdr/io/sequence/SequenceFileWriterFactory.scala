package uk.gov.ons.mdr.io.sequence

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.hadoop.io.{BytesWritable, SequenceFile, Text}

trait WriterFactory {
  def createWriter(fileName: String): Writer
}

trait SequenceFileWriterFactory extends WriterFactory {

  def createWriter(fileName: String): Writer = {

    val conf = new Configuration()
    fs.FileSystem.getLocal(conf)

    val path = new fs.Path(fileName)

    SequenceFile.createWriter(conf,
      Writer.file(path),
      Writer.keyClass(classOf[Text]),
      Writer.valueClass(classOf[BytesWritable]),
      Writer.compression(CompressionType.NONE))
  }
}
