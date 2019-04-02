package uk.gov.ons.mdr.io

import org.apache.hadoop.io.{BytesWritable, Text}

class SingleSequenceWriter(dirPath: String, sourceFile: String) extends SequenceWriter {

  this: WriterFactory =>

  private var writer = createWriter(s"$dirPath/${sourceFile.stripSuffix(".zip")}.seq")

  override def write(fileData: FileData): Unit = {
    writer.append(new Text(fileData.name), new BytesWritable(fileData.content))
  }

  override def close(): Unit = {
    writer.close()
  }
}
