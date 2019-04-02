package uk.gov.ons.mdr.io

import java.util.Date
import java.util.zip.ZipFile

import com.google.common.io.ByteStreams

import scala.collection.JavaConversions._

class ZipReader(file: String) {

  def iterator(): Iterator[FileData] = {
    val zipFile = new ZipFile(file)

    val zipIterator = for (entry <- zipFile.entries) yield {
      val is = zipFile.getInputStream(entry)
      val bytes = ByteStreams.toByteArray(is)

      FileData(entry.getName, bytes)
    }

    val loggingIterator = for ((p, c) <- zipIterator.zipWithIndex) yield {
      if (c % 1000 == 0) {
        println(s"${new Date()} File iterator at $c")
      }
      p
    }

    loggingIterator
  }
}
