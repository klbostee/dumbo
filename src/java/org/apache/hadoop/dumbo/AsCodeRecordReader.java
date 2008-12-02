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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

/**
 * A record reader that converts to Dumbo code.
 */
public class AsCodeRecordReader implements RecordReader<Text, Text> {

  private RecordReader<Writable, Writable> realRecordReader;
  private Writable realKey, realValue;
  private String filenameCode = null;

  public AsCodeRecordReader(RecordReader<Writable, Writable> realRecordReader, String filename) {
    this.realRecordReader = realRecordReader;
    realKey = realRecordReader.createKey();
    realValue = realRecordReader.createValue();
    if (filename != null) filenameCode = CodeUtils.stringToCode(filename);
  }

  public void close() throws IOException {
    realRecordReader.close();
  }

  public Text createKey() {
    return new Text();
  }

  public Text createValue() {
    return new Text();
  }

  public long getPos() throws IOException {
    return realRecordReader.getPos();
  }

  public float getProgress() throws IOException {
    return realRecordReader.getProgress();
  }

  public boolean next(Text key, Text value) throws IOException {
    if (!realRecordReader.next(realKey, realValue)) return false;
    if (filenameCode != null) {
      key.set(CodeUtils.codesToTuple(filenameCode, CodeUtils.writableToCode(realKey)));
    } else {
      key.set(CodeUtils.writableToCode(realKey));
    }
    value.set(CodeUtils.writableToCode(realValue));
    return true;
  }

}
