/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.dumbo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class implements a program that can be used to print a DFS path,
 * after applying one of the Dumbo InputFormats to it.
 */
public class CatPath extends Configured implements Tool {

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    JobConf job = new JobConf(conf);

    FileSystem fs = FileSystem.get(conf);
    Path inputPath = new Path(args[1]);
    List<FileStatus> inputFiles = new ArrayList<FileStatus>(); 

    FileStatus status = fs.getFileStatus(inputPath);
    if(status.isDir()) {
      FileStatus[] files = fs.listStatus(inputPath);
      Collections.addAll(inputFiles, files);
    } else {
      inputFiles.add(status);
    }

    for (FileStatus fileStatus : inputFiles) {
      FileSplit split = new FileSplit(fileStatus.getPath(), 0, 
          fileStatus.getLen()*fileStatus.getBlockSize(), (String[])null);
      InputFormat<Text,Text> inputformat;
      if (args[0].toLowerCase().equals("sequencefileasnamedcode")) {
        inputformat = new SequenceFileAsCodeInputFormat(true);
      } else if (args[0].toLowerCase().equals("sequencefileascode")) {
        inputformat = new SequenceFileAsCodeInputFormat(false);
      } else if (args[0].toLowerCase().equals("textasnamedcode")) {
        inputformat = new TextAsCodeInputFormat(true);
      } else if (args[0].toLowerCase().equals("textascode")) {
        inputformat = new TextAsCodeInputFormat(false);
      } else {
        AsCodeInputFormat asCodeInputformat;
        if (args[0].toLowerCase().equals("asnamedcode")) {
          asCodeInputformat = new AsCodeInputFormat(true); 
        } else {
          asCodeInputformat = new AsCodeInputFormat(false); 
        }
        asCodeInputformat.configure(job);
        inputformat = asCodeInputformat;
      }
      RecordReader<Text,Text> reader = inputformat.getRecordReader(split, job, Reporter.NULL);
      Text key = new Text(), value = new Text();
      while(reader.next(key, value)) {
        System.out.println(key.toString() + "\t" + value.toString());
      }
      reader.close();
    }

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CatPath(), args);
    System.exit(res);
  }
}
