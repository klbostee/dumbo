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
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.TreeMap;

import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.record.Record;
import org.apache.hadoop.record.RecordOutput;

/**
 * This class serializes records to Dumbo code.
 */
public class CodeRecordOutput implements RecordOutput {

  private PrintStream stream;
  private boolean isFirst = true;
  private boolean inMap = false;
  private boolean isMapKey = true;

  private void printCommaUnlessFirst() {
    if (!isFirst) stream.print(",");
    isFirst = false;
  }

  private void printSeparatorIfNeeded() {
    if (!inMap) {
      printCommaUnlessFirst();
    } else {
      if (isMapKey) printCommaUnlessFirst();
      else stream.print(":");
      isMapKey = !isMapKey;
    }
  }

  public CodeRecordOutput(OutputStream out) {
    try {
      stream = new PrintStream(out, true, "UTF-8");
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }
  }

  public void startMap(TreeMap m, String tag) throws IOException {
    printSeparatorIfNeeded();
    isFirst = true;
    inMap = true;
    isMapKey = true;
    stream.print("{");
  }

  public void endMap(TreeMap m, String tag) throws IOException {
    stream.print("}");
    isFirst = false;
    inMap = false;
    isMapKey = true;
  }

  public void startRecord(Record r, String tag) throws IOException {
    printSeparatorIfNeeded();
    isFirst = true;
    stream.print("(");
  }

  public void endRecord(Record r, String tag) throws IOException {
    stream.print(")");
    isFirst = false;
  }

  public void startVector(ArrayList v, String tag) throws IOException {
    printSeparatorIfNeeded();
    isFirst = true;
    stream.print("[");
  }

  public void endVector(ArrayList v, String tag) throws IOException {
    stream.print("]");
    isFirst = false;
  }

  public void writeBool(boolean b, String tag) throws IOException {
    printSeparatorIfNeeded();
    stream.print(CodeUtils.booleanToCode(b));
  }

  public void writeBuffer(Buffer buf, String tag) throws IOException {
    printSeparatorIfNeeded();
    stream.print(CodeUtils.stringToCode(buf.toString()));
  }

  public void writeByte(byte b, String tag) throws IOException {
    printSeparatorIfNeeded();
    stream.print(CodeUtils.stringToCode((new Byte(b)).toString()));
  }

  public void writeDouble(double d, String tag) throws IOException {
    printSeparatorIfNeeded();
    stream.print(CodeUtils.numberToCode(d));
  }

  public void writeFloat(float f, String tag) throws IOException {
    printSeparatorIfNeeded();
    stream.print(CodeUtils.numberToCode(f));
  }

  public void writeInt(int i, String tag) throws IOException {
    printSeparatorIfNeeded();
    stream.print(CodeUtils.numberToCode(i));
  }

  public void writeLong(long l, String tag) throws IOException {
    printSeparatorIfNeeded();
    stream.print(CodeUtils.numberToCode(l));
  }

  public void writeString(String s, String tag) throws IOException {
    printSeparatorIfNeeded();
    stream.print(CodeUtils.stringToCode(s));
  }
}
