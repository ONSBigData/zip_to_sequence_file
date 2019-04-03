package uk.gov.ons.mdr.io.sequence

import java.io.File

case class Config(
                   sourceFile: ZipInputFile = ZipInputFile(new File("")),
                   targetDir: SequenceOutputDir = SequenceOutputDir(new File(".")),
                   singleFileMode: Boolean = false,
                   targetBytes: Long = Long.MaxValue
                 )

case class FileData(name: String, content: Array[Byte])

case class SequenceOutputDir(file: File) extends AnyVal

case class ZipInputFile(file: File) extends AnyVal