package uk.gov.ons.mdr.io.sequence

import java.nio.file.{Path, Paths}

/** Config (parsed from command line) */
case class Config(
                   sourceFile: ZipInputFile = ZipInputFile(Paths.get("")),
                   targetDir: SequenceOutputDir = SequenceOutputDir(Paths.get(".")),
                   singleFileMode: Boolean = false,
                   targetBytes: Long = Long.MaxValue
                 )

case class FileData(name: String, content: Array[Byte])

case class SequenceOutputDir(path: Path) extends AnyVal

case class ZipInputFile(path: Path) extends AnyVal