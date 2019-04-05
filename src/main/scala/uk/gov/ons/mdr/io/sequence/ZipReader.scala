package uk.gov.ons.mdr.io.sequence

import java.io.Closeable
import java.nio.file.Path
import java.util.zip.ZipFile

import com.google.common.io.ByteStreams
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

/** Provides an iterator for a zip archive */
class ZipReader(path: Path) extends Closeable {

  def logger: Logger = LoggerFactory.getLogger(this.getClass)

  val zipFile = new ZipFile(path.toFile)

  def iterator(): Iterator[FileData] = {

    val zipIterator = for (entry <- zipFile.entries) yield {
      val is = zipFile.getInputStream(entry)
      val bytes = ByteStreams.toByteArray(is)

      FileData(entry.getName, bytes)
    }

    val loggingIterator = for ((fileData, index) <- zipIterator.zipWithIndex) yield {
      if (index % 1000 == 0) {
        logger.info(s"zip iterator for ${zipFile.getName} at entry $index")
      }
      fileData
    }

    loggingIterator
  }

  override def close(): Unit = zipFile.close()
}
