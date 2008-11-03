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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.python.core.PyBoolean;
import org.python.core.PyDictionary;
import org.python.core.PyFloat;
import org.python.core.PyInteger;
import org.python.core.PyList;
import org.python.core.PyLong;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.core.PyTuple;

import junit.framework.TestCase;

/**
 * This class tests PyObjectWritable serialization.
 */
public class TestPyObjectWritable extends TestCase {

  public TestPyObjectWritable(String testName) {
    super(testName);
  }
  
  protected void setUp() throws Exception {
  }

  protected void tearDown() throws Exception {
  }
  
  public void testBoolean() throws Exception {
    testPyObject(new PyBoolean(true));
    testPyObject(new PyBoolean(false));
  }
  
  public void testNumber() throws Exception {
    testPyObject(new PyInteger(1234));
    testPyObject(new PyLong(1234567));
    testPyObject(new PyFloat(123.45));
  }
  
  public void testString() throws Exception {
    testPyObject(new PyString("random text"));
  }
  
  public void testContainer() throws Exception {
    PyObject po1 = new PyInteger(1234);
    PyObject po2 = new PyString("random text");
    testPyObject(new PyTuple(new PyObject[] { po1, po2 }));
    testPyObject(new PyList(new PyObject[] { po1, po2 }));
    PyDictionary pydict = new PyDictionary();
    pydict.__setitem__(po1, po2);
    testPyObject(pydict);
  }
  
  private static void testPyObject(PyObject pyobj) throws IOException {
    PyObject before = pyobj;
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(bout);
    PyObjectWritable pow = new PyObjectWritable(pyobj);
    pow.write(dout);
    dout.close();
    bout.close();
    ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
    DataInputStream din = new DataInputStream(bin);
    pow.readFields(din);
    din.close();
    bin.close();
    PyObject after = pow.get();
    assertTrue("Wrong PyObject deserialized for " + pyobj.getClass().getName() + ".", 
        before.toString().equals(after.toString()));
  }
}
