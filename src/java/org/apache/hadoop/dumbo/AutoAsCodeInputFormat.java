package org.apache.hadoop.dumbo;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * An input format that tries to decide automatically if the input is a sequence file or a text
 * file and then converts to Dumbo code.
 */
public class AutoAsCodeInputFormat extends FileInputFormat<Text,Text> implements JobConfigurable {
  
  private final SequenceFileAsCodeInputFormat sequenceFileInputFormat;
  private final TextAsCodeInputFormat textInputFormat; 

  public AutoAsCodeInputFormat(boolean named) {
    sequenceFileInputFormat = new SequenceFileAsCodeInputFormat(named);
    textInputFormat = new TextAsCodeInputFormat(named);
  }
  
  public AutoAsCodeInputFormat() {
    this(false);
  }
  
  public void configure(JobConf job) {
    sequenceFileInputFormat.configure(job);
    textInputFormat.configure(job);
  }
  
  public RecordReader<Text,Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    FileSplit fileSplit = (FileSplit) split;
    FileSystem fs = FileSystem.get(job);
    FSDataInputStream is = fs.open(fileSplit.getPath());
    byte[] header = new byte[3];
    is.readFully(header);
    is.close();
    if (header[0] == 'S' && header[1] == 'E' && header[2] == 'Q') {
      return sequenceFileInputFormat.getRecordReader(split, job, reporter);
    } else {
      return textInputFormat.getRecordReader(split, job, reporter);
    }
  }
  
}
