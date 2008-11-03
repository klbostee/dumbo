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

import junit.framework.TestCase;

/**
 * This class tests the conversion to PyObjects.
 */
public class TestPyObjectUtils extends TestCase {

  public TestPyObjectUtils(String testName) {
    super(testName);
  }
  
  protected void setUp() throws Exception {
  }

  protected void tearDown() throws Exception {
  }
  
  public void testBoolean() {
    assertTrue("Wrong PyObject generated for boolean.", 
        "True".equals(PyObjectUtils.booleanToPyObject(true).toString()));
    assertTrue("Wrong PyObject generated for boolean.", 
        "False".equals(PyObjectUtils.booleanToPyObject(false).toString()));
  }
  
  public void testNumber() {
    assertTrue("Wrong PyObject generated for integer.", 
        "1234".equals(PyObjectUtils.integerToPyObject(1234).toString()));
    assertTrue("Wrong PyObject generated for double.", 
        "1.234".equals(PyObjectUtils.doubleToPyObject(1.234).toString()));
  }
  
  public void testString() {
    assertTrue("Wrong PyObject generated for String.", 
        "random".equals(PyObjectUtils.stringToPyObject("random").toString()));
  }
}
