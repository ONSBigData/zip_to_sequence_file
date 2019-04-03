# Sequencer

Command line utility to convert Zip archives into Hadoop Sequence Files

To build:

```mvn package```

The utility is packaged as a jar file: `sequencer.jar`.


```
Usage: java -jar sequencer.jar [options] <zip_file>

e.g. java -jar sequencer.jar -s some_archive.zip

  -t, --target <dir>       Target directory for sequence file creation, default: SequenceOutputDir(.)
  -s, --singleFileMode     Flag to run in singleFileMode
  -b, --batchSize <string>
                           When not in singleFileMode controls the batch size for the sequence file (e.g. 200m, 1g)
  <zip_file>               Zip archive for conversion
  --help                   Show help text
```