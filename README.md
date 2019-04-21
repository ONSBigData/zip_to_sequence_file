# Sequencer

Command line utility to convert Zip archives into Hadoop Sequence Files

To build:

```mvn package```

The utility is packaged as a jar file: `sequencer.jar`

```
Usage: java -jar sequencer.jar [options] <zip_file>

e.g. java -jar sequencer.jar some_archive.zip

  -t, --target <dir>       Target directory for sequence file creation, default: .
  -b, --batchSize <string>
                           Optional approximate sequence batch size (e.g. 200m, 1g)
  <zip_file>               Zip archive for conversion
  --help                   Show help text
```

Default behaviour is to produce a single sequence file.
Setting the `-b` argument will switch to batch mode, producing sequence files of roughly the specified batch size.