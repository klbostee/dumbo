/*
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
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.python.core.PyInteger;
import org.python.core.PyLong;
import org.python.core.PyString;

/**
 * This program prints the number of bytes used for certain PyObjects
 * by PyObjectWritable.
 */
public class PyObjectWritableLengths {

  public static void integerLength(int i) throws IOException {
    System.out.println("Lengths for integer '" + i + "':");
    printLength(new PyObjectWritable(new PyInteger(i)));
    printLength(new IntWritable(i));
    printLength(new VIntWritable(i));
  }
  
  public static void longLength(long l) throws IOException {
    System.out.println("Lengths for long '" + l + "':");
    printLength(new PyObjectWritable(new PyLong(l)));
    printLength(new LongWritable(l));
    printLength(new VLongWritable(l));
  }
  
  public static void stringLength(String s) throws IOException {
    System.out.println("Lengths for string '" + s + "':");
    printLength(new PyObjectWritable(new PyString(s)));
    printLength(new Text(s));
  }
  
  private static void printLength(Writable writable) throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(bout);
    writable.write(dout);
    dout.close();
    bout.close();
    System.out.println("  " + writable.getClass().getName() + "\t" + bout.toByteArray().length + " bytes");
  }
  
  public static void main(String[] args) throws IOException {
    integerLength(12);
    integerLength(123456789);
    longLength(12345678912345689L);
    stringLength("random text");
  }
}
