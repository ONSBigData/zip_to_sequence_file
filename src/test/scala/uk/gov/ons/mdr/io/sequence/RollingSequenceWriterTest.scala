package uk.gov.ons.mdr.io.sequence

import java.nio.file.{Path, Paths}

import org.apache.hadoop.io.{BytesWritable, SequenceFile, Text, Writable}
import org.apache.hadoop.io.SequenceFile.Writer
import org.mockito.Matchers
import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.scalatest.mockito.MockitoSugar

import org.scalatest.Assertions._

import scala.collection.mutable

class RollingSequenceWriterTest extends FlatSpec with MockitoSugar {

  "RollingSequenceWriter" should "not close writer for a small batch" in {

    trait MockWriterFactory extends WriterFactory {

      object MockHolder {
        val mockWriter = mock[SequenceFile.Writer]
      }
      def mockWriter: Writer = MockHolder.mockWriter

      def createWriter(path: Path): Writer = {
        mockWriter
      }
    }

    val rollingWriter = new RollingSequenceWriter(
      SequenceOutputDir(Paths.get(".")),
      "some_prefix",
      Long.MaxValue) with MockWriterFactory

    val testData = (1 to 100).map(_ => FileData("some_content", "Some value".getBytes))

    // When
    testData.foreach(rollingWriter.write)

    // Then
    verify(rollingWriter.mockWriter, atLeastOnce).append(Matchers.any[Writable], Matchers.any[Writable])
    verify(rollingWriter.mockWriter, never).close()
  }

  "RollingSequenceWriter" should "close writers for larger batches" in {

    trait MockWriterFactory extends WriterFactory {

      object MockHolder {
        val mockWriter = mock[SequenceFile.Writer]
      }
      def mockWriter: Writer = MockHolder.mockWriter

      def createWriter(path: Path): Writer = {
        mockWriter
      }
    }

    val rollingWriter = new RollingSequenceWriter(
      SequenceOutputDir(Paths.get(".")),
      "some_prefix",
      550L) with MockWriterFactory

    val testData = (1 to 100).map(_ => FileData("some_content", "Some value".getBytes))

    // When
    testData.foreach(rollingWriter.write)

    // Then
    verify(rollingWriter.mockWriter, times(3)).close()
  }

  "RollingSequenceWriter" should "use unique filenames for batches" in {

    trait MockWriterFactory extends WriterFactory {

      object MockHolder {
        var pathNames = new mutable.MutableList[String]()
        val mockWriter: Writer = mock[SequenceFile.Writer]
      }
      def mockWriter: Writer = MockHolder.mockWriter

      def createWriter(path: Path): Writer = {
        MockHolder.pathNames += path.getFileName.toString
        mockWriter
      }
    }

    val rollingWriter = new RollingSequenceWriter(
      SequenceOutputDir(Paths.get(".")),
      "some_prefix",
      10L) with MockWriterFactory

    val testData = (1 to 10).map(_ => FileData("some_content", "Some value".getBytes))

    // When
    testData.foreach(rollingWriter.write)

    // Then
    val fileNameListLength = rollingWriter.MockHolder.pathNames.length
    val numberOfDistinctNames = rollingWriter.MockHolder.pathNames.toSet.size

    assert(fileNameListLength > 0)
    assert(fileNameListLength == numberOfDistinctNames)
  }

}
