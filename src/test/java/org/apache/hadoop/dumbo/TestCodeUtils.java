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

import org.apache.hadoop.dumbo.CodeUtils;

/**
 * This class tests the generation of Dumbo code.
 */
public class TestCodeUtils extends TestCase {

  public TestCodeUtils(String testName) {
    super(testName);
  }

  protected void setUp() throws Exception {
  }

  protected void tearDown() throws Exception {
  }

  public void testBoolean() {
    assertTrue("Generated code for boolean wrong.", 
        "True".equals(CodeUtils.booleanToCode(true)));
    assertTrue("Generated code for boolean wrong.", 
        "False".equals(CodeUtils.booleanToCode(false)));
  }

  public void testNumber() {
    assertTrue("Generated code for number wrong.",
        "1234".equals(CodeUtils.intToCode(1234)));
    assertTrue("Generated code for number wrong.",
        "1.23".equals(CodeUtils.doubleToCode(1.23)));
  }

  public void testString() {
    assertTrue("Generated code for string wrong.", 
        "'test\\tline\\nline2'".equals(CodeUtils.stringToCode("test\tline\nline2")));
  }

}
