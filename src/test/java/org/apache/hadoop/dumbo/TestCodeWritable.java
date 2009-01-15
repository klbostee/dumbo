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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

/**
 * This class tests CodeWritable serialization.
 */
public class TestCodeWritable extends TestCase {

  public TestCodeWritable(String testName) {
    super(testName);
  }

  protected void setUp() throws Exception {
  }

  protected void tearDown() throws Exception {
  }

  public void testNone() throws Exception {
    testCode("None");
  }

  public void testBoolean() throws Exception {
    testCode("True");
    testCode("False");
  }

  public void testNumber() throws Exception {
    testCode("1234");
    testCode("1234L", false);
    testCode("123.45");
  }

  public void testString() throws Exception {
    testCode("''");
    testCode("'random text'");
  }

  public void testContainer() throws Exception {
    testCode("(1,2,3,'random')");
    testCode("( 1, 2, 3, 'random' )","(1,2,3,'random')");
    testCode("[1,2,3,'random']");
    testCode("{'1':1,'2':2}");
    testCode("('test','paul\\'s, mine, and yours')");
    testCode("('test',\"paul's, mine, and yours\")","('test','paul\\'s, mine, and yours')");
    testCode("([(1,2),(3,4)],[(5,6),(7,8)],(9,10),(11,12),13)");
    testCode("(1,)", false);
  }

  public void testRest() throws Exception {
    testCode("MyClass(1,'string')", false);
    testCode("datetime.datetime(2009,1,2,13,26,28)", false);
  }

  private static void testCode(String code, String goal, boolean strict) throws IOException {
    testSerialization(code, goal);
    compareWithText(code, strict);
  }

  private static void testCode(String code, String goal) throws IOException {
    testCode(code, goal, true);
  }

  private static void testCode(String code, boolean strict) throws IOException {
    testCode(code, code, strict);
  }

  private static void testCode(String code) throws IOException {
    testCode(code, code, true);
  }

  private static void testSerialization(String code, String goal) throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(bout);
    CodeWritable cw = new CodeWritable(code);
    cw.write(dout);
    dout.close();
    bout.close();
    ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
    DataInputStream din = new DataInputStream(bin);
    cw.readFields(din);
    din.close();
    bin.close();
    String after = cw.get();
    System.out.println("before: " + code + ", after: " + after);
    assertTrue("Wrong code deserialized for \"" + code + "\".", goal.equals(after));
  }

  private static void compareWithText(String code, boolean strict) throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(bout);
    CodeWritable cw = new CodeWritable(code);
    cw.write(dout);
    dout.close();
    bout.close();
    int cwlen = bout.toByteArray().length;
    bout = new ByteArrayOutputStream();
    dout = new DataOutputStream(bout);
    Text text = new Text(code);
    text.write(dout);
    dout.close();
    bout.close();
    int textlen = bout.toByteArray().length;
    System.out.println("Number of bytes for \"" + code + "\" when using CodeWritable = " + cwlen);
    System.out.println("Number of bytes for \"" + code + "\" when using Text = " + textlen);
    if (strict)
      assertTrue("CodeWritable does not require less bytes than Text for \"" + code + "\"", cwlen < textlen);
    else
      assertTrue("Text requires at least 2 bytes less than CodeWritable for \"" + code + "\"", cwlen - 1 <= textlen);
  }
}
