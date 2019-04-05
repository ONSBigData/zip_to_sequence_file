package uk.gov.ons.mdr.io

/** Classes for converting zip archives into Hadoop [[org.apache.hadoop.io.SequenceFile]].
  *
  * == Overview ==
  * The main entry point is [[uk.gov.ons.mdr.io.sequence.Sequencer]], this class will be called when
  * the jar is run from the command line.
  *
  * == Class Summary ==
  * [[uk.gov.ons.mdr.io.sequence.ZipToSequence]]
  * Passes the contents of a reader to a writer.
  *
  * [[uk.gov.ons.mdr.io.sequence.ZipReader]]
  * Provides an iterator for the data stored in a zip archive.
  *
  * [[uk.gov.ons.mdr.io.sequence.SingleSequenceWriter]]
  * A simple writer which will store data in a single sequence file.
  *
  * [[uk.gov.ons.mdr.io.sequence.RollingSequenceWriter]]
  * Stores input in a number of files, has a setting for the amount of data to store before rolling the file.
  *
  * [[uk.gov.ons.mdr.io.sequence.SequenceFileWriterFactory]]
  * Handles the creation of the underlying Hadoop [[org.apache.hadoop.io.SequenceFile.Writer]] used by
  * [[uk.gov.ons.mdr.io.sequence.SingleSequenceWriter]] and [[uk.gov.ons.mdr.io.sequence.RollingSequenceWriter]].
  */
package object sequence {

}
