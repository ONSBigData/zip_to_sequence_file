package uk.gov.ons.mdr.io

case class Config(
                   sourceFile: String = "",
                   targetDir: String = ".",
                   singleFileMode: Boolean = false,
                   targetBytes: Long = Long.MaxValue
                 )

case class FileData(name: String, content: Array[Byte])
