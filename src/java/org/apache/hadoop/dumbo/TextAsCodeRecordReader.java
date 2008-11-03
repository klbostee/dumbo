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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

/**
 * A record reader that converts text records to Dumbo code.
 */
public class TextAsCodeRecordReader implements RecordReader<Text, Text> {

  private final LineRecordReader lineRecordReader;
  private final String filenameCode;
  private final boolean filenameInKey;
  private final boolean offsetInKey;

  public TextAsCodeRecordReader(Configuration job, FileSplit split,
      boolean filenameInKey, boolean offsetInKey) throws IOException { 
    lineRecordReader = new LineRecordReader(job, split);
    filenameCode = CodeUtils.stringToCode(split.getPath().getName());
    this.filenameInKey = filenameInKey;
    this.offsetInKey = offsetInKey;
  }

  public TextAsCodeRecordReader(Configuration job, FileSplit split) throws IOException {
    this(job, split, true, true);
  }

  public void close() throws IOException {
    lineRecordReader.close();
  }

  public Text createKey() {
    return new Text();
  }

  public Text createValue() {
    return new Text();
  }

  public long getPos() throws IOException {
    return lineRecordReader.getPos();
  }

  public float getProgress() throws IOException {
    return lineRecordReader.getProgress();
  }

  public boolean next(Text key, Text value) throws IOException {
    LongWritable lw = new LongWritable();
    if (!lineRecordReader.next(lw, value)) return false;
    if (offsetInKey) {
      String offsetCode = CodeUtils.numberToCode(lw.get());
      key.set(filenameInKey ? 
          CodeUtils.combineCodes(filenameCode, offsetCode) : offsetCode);
    } else if (filenameInKey) { 
      key.set(filenameCode); 
    } else { 
      key.set(CodeUtils.NULL_CODE); 
    }
    value.set(CodeUtils.stringToCode(value.toString()));
    return true;
  }

}
