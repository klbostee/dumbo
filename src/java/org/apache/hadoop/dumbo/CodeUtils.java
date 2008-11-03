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

import java.io.ByteArrayOutputStream;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.record.Record;

/**
 * This class provides functions that generate Dumbo code.
 */
public abstract class CodeUtils {

  private CodeUtils() {}

  public static final String NULL_CODE = "None";

  public static String booleanToCode(Boolean b) {
    return b ? "True" : "False";
  }

  public static String numberToCode(Number n) {
    return n.toString();
  }

  public static String stringToCode(String s) {
    return "'" + s.replace("\\", "\\\\")
    .replace("\n", "\\n")
    .replace("\r", "\\r")
    .replace("\t", "\\t")
    .replace("'", "\\'")
    + "'";
  }

  public static String recordToCode(Record r) {
    try {
      ByteArrayOutputStream s = new ByteArrayOutputStream();
      r.serialize(new CodeRecordOutput(s));
      return new String(s.toByteArray(), "UTF-8");
    } catch (Throwable ex) {
      throw new RuntimeException(ex);
    }
  }

  public static String writableToCode(Writable w) {
    // DoubleWritable not handled (yet) because it is not available
    // in older Hadoop versions...
    if (w instanceof BooleanWritable) 
      return booleanToCode(((BooleanWritable)w).get());
    else if (w instanceof IntWritable) 
      return numberToCode(((IntWritable)w).get());
    else if (w instanceof LongWritable) 
      return numberToCode(((LongWritable)w).get());
    else if (w instanceof FloatWritable) 
      return numberToCode(((FloatWritable)w).get());
    else if (w instanceof Text) 
      return stringToCode(((Text)w).toString());
    else if (w instanceof Record) 
      return recordToCode((Record)w);
    else return stringToCode(w.toString());
  }

  public static String combineCodes(String code1, String code2) {
    return "(" + code1 + "," + code2 + ")";
  }
}
