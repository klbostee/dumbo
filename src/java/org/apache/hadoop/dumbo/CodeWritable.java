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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.dumbo.CodeUtils.CodeType;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * A writable that stores Dumbo code efficiently.
 */
public class CodeWritable implements WritableComparable {

  private String code = null;
  
  public CodeWritable(String code) {
    if (code != null) this.code = code.trim();
  }
  
  public CodeWritable() {
    this(null);
  }
  
  public void set(String code) {
    this.code = code.trim();
  }
  
  public String get() {
    return code;
  }
  
  public CodeType getType() {
  	return CodeUtils.deriveType(code);
  }
  
  public void write(DataOutput out) throws IOException {
    CodeType type = CodeUtils.deriveType(code);
    out.writeByte(type.ordinal());
    if (type == CodeType.STRING) {
    	WritableUtils.writeString(out, CodeUtils.codeToString(code));
    } else if (type == CodeType.BOOLEAN) {
      out.writeBoolean(CodeUtils.codeToBoolean(code));
    } else if (type == CodeType.INTEGER) {
      WritableUtils.writeVInt(out, CodeUtils.codeToInt(code));
    } else if (type == CodeType.LONG) {
      WritableUtils.writeVLong(out, CodeUtils.codeToLong(code));
    } else if (type == CodeType.FLOAT) {
      out.writeFloat(CodeUtils.codeToFloat(code));
    } else if (type == CodeType.TUPLE) {
      writeSequence(out, CodeUtils.codesFromTuple(code));
    } else if (type == CodeType.LIST) {
      writeSequence(out, CodeUtils.codesFromList(code));
    } else if (type != CodeType.NULL) {
      WritableUtils.writeString(out, code); // write code itself
    }
  }
  
  public void readFields(DataInput in) throws IOException {
    int type = in.readByte();
    if (type == CodeType.STRING.ordinal()) {
      code = CodeUtils.stringToCode(WritableUtils.readString(in));
    } else if (type == CodeType.BOOLEAN.ordinal()) {
      code = CodeUtils.booleanToCode(in.readBoolean());
    } else if (type == CodeType.INTEGER.ordinal()) {
      code = CodeUtils.intToCode(WritableUtils.readVInt(in));
    } else if (type == CodeType.LONG.ordinal()) {
      code = CodeUtils.longToCode(WritableUtils.readVLong(in));
    } else if (type == CodeType.FLOAT.ordinal()) {
      code = new Float(in.readFloat()).toString();
    } else if (type == CodeType.TUPLE.ordinal()){
      code = CodeUtils.codesToTuple(readSequence(in));
    } else if (type == CodeType.LIST.ordinal()){
      code = CodeUtils.codesToList(readSequence(in));
    } else if (type == CodeType.NULL.ordinal()) {
      code = CodeUtils.NULL_CODE;
    } else {
      code = WritableUtils.readString(in);
    }
  }

  private static void writeSequence(DataOutput out, String[] codes) throws IOException {
    WritableUtils.writeVInt(out, codes.length);
    for (String subcode : codes) { 
    	(new CodeWritable(subcode)).write(out);
    }
  }
  
  private static String[] readSequence(DataInput in) throws IOException {
    int length = WritableUtils.readVInt(in);
    String[] codes = new String[length];
    for (int i = 0; i < length; i++) {
    	CodeWritable cw = new CodeWritable();
    	cw.readFields(in);
    	codes[i] = cw.get();
    }
    return codes;
  }
  
  public int compareTo(Object obj) {
    return code.compareTo(((CodeWritable)obj).code);
  }

  public int hashCode() {
    return code.hashCode();
  }
  
  public String toString() {
    return code;
  }
  
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(CodeWritable.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      int type1 = b1[s1], type2 = b2[s2];
      if (type1 != type2) return type1 < type2 ? -1 : 1;
      return compareBytes(b1, s1+1, l1-1, b2, s2+1, l2-1);
    }
  }

  static {
    WritableComparator.define(CodeWritable.class, new Comparator());
  }
}
