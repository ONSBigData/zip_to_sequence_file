package uk.gov.ons.mdr.io.sequence


import java.nio.file.Paths

import org.apache.hadoop.io.{BytesWritable, Text}

class SingleSequenceWriter(sequenceOutputDir: SequenceOutputDir, zipInputFile: ZipInputFile) extends SequenceWriter {

  this: WriterFactory =>

  val dirString = sequenceOutputDir.path.toString
  val outFileName = zipInputFile.path.getFileName.toString.stripSuffix(".zip") + ".seq"

  val outPath = Paths.get(dirString, outFileName)

  private var writer = createWriter(outPath)

  override def write(fileData: FileData): Unit = {
    writer.append(new Text(fileData.name), new BytesWritable(fileData.content))
  }

  override def close(): Unit = {
    writer.close()
  }
}
