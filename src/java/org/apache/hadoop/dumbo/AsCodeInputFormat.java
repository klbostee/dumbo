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

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * An input format that converts to Dumbo code.
 */
public class AsCodeInputFormat implements InputFormat<Text,Text>, JobConfigurable {

  private InputFormat realInputFormat = null;
  private boolean named = false;

  public AsCodeInputFormat(InputFormat realInputFormat, boolean named) {
    this.realInputFormat = realInputFormat;
    this.named = named;
  }

  public AsCodeInputFormat(InputFormat realInputFormat) {
    this(realInputFormat, false);
  }

  public AsCodeInputFormat(boolean named) {
    this(null, named);
  }

  public AsCodeInputFormat() {
    this(null, false);
  }

  public void configure(JobConf job) {
    if (realInputFormat == null) {
      Class<? extends InputFormat> realInputFormatClass 
      = job.getClass("dumbo.as.code.input.format.class", TextInputFormat.class, InputFormat.class);
      try {
        realInputFormat = realInputFormatClass.newInstance();
        for (Class interface_ : realInputFormatClass.getInterfaces()) {
          if (interface_.equals(JobConfigurable.class)) {
            JobConfigurable jc = (JobConfigurable) realInputFormat;
            jc.configure(job);
            break;
          }
        }
      } catch (InstantiationException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
    if (!named) {
      named = job.getBoolean("dumbo.as.named.code", false);
    }
  }

  @SuppressWarnings("unchecked")
  private RecordReader<Text,Text> createRecordReader(InputSplit split, JobConf job,
      Reporter reporter, String filename) throws IOException {
    return new AsCodeRecordReader(realInputFormat.getRecordReader(split, job, reporter), filename);
  }

  public RecordReader<Text,Text> getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    String filename = null;
    if (named && split instanceof FileSplit) {
      filename = ((FileSplit) split).getPath().getName();
    }
    return createRecordReader(split, job, reporter, filename);
  }

  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    return realInputFormat.getSplits(job, numSplits);
  }

  public void validateInput(JobConf job) throws IOException {
    realInputFormat.validateInput(job);
  }

}
