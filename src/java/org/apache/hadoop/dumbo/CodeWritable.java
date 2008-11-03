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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * A writable that stores Dumbo code efficiently.
 */
public class CodeWritable implements WritableComparable {

  private String code = null;
  
  private static enum FieldType {
    NONE, BOOLEAN, INTEGER, LONG, FLOAT, STRING, TUPLE, LIST, DICTIONARY
  }
  
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
  
  public void write(DataOutput out) throws IOException {
    char first = code.charAt(0);
    if (first == 'N') {
      out.writeByte(FieldType.NONE.ordinal());
    } else if (first == 'T' || first == 'F') {
      out.writeByte(FieldType.BOOLEAN.ordinal());
      out.writeBoolean(first == 'T');
    } else if (first == '\'' || first == '"') {
      out.writeByte(FieldType.STRING.ordinal());
      WritableUtils.writeString(out, code.substring(1, code.length()-1));
    } else if (first == '(') {
      out.writeByte(FieldType.TUPLE.ordinal());
      writeSequence(out);
    } else if (first == '[') {
      out.writeByte(FieldType.LIST.ordinal());
      writeSequence(out);
    } else if (first == '{') {
      out.writeByte(FieldType.DICTIONARY.ordinal());
      // TODO: make this more efficient
      WritableUtils.writeString(out, code);
    } else if (code.contains(".")) {
      out.writeByte(FieldType.FLOAT.ordinal());
      out.writeFloat(Float.parseFloat(code));
    } else if (code.charAt(code.length()-1) == 'L') {
      out.writeByte(FieldType.LONG.ordinal());
      WritableUtils.writeVLong(out, Long.parseLong(code.substring(0, code.length()-1)));
    } else {
      out.writeByte(FieldType.INTEGER.ordinal());
      WritableUtils.writeVInt(out, Integer.parseInt(code));
    }
  }
  
  private void writeSequence(DataOutput out) throws IOException {
    List<CodeWritable> items = new ArrayList<CodeWritable>();
    boolean inStr = false;
    int prevIndex = 1; 
    for (int i = 1; i < code.length()-1; i++) {
      char c = code.charAt(i);
      if (c == '\'' || c == '"') inStr = !inStr;
      else if (!inStr && c == ',') {
        items.add(new CodeWritable(code.substring(prevIndex, i).trim()));
        prevIndex = i+1;
      }
    }
    items.add(new CodeWritable(code.substring(prevIndex, code.length()-1).trim()));
    WritableUtils.writeVInt(out, items.size());
    Iterator<CodeWritable> it = items.iterator();
    while (it.hasNext()) it.next().write(out);
  }
  
  public void readFields(DataInput in) throws IOException {
    int type = in.readByte();
    if (type == FieldType.NONE.ordinal()) {
      code = "None";
    } else if (type == FieldType.BOOLEAN.ordinal()) {
      if (in.readBoolean()) code = "True";
      else code = "False";
    } else if (type == FieldType.STRING.ordinal()) {
      code = "'" + WritableUtils.readString(in) + "'";
    } else if (type == FieldType.INTEGER.ordinal()) {
      code = new Integer(WritableUtils.readVInt(in)).toString();
    } else if (type == FieldType.LONG.ordinal()) {
      code = new Long(WritableUtils.readVLong(in)).toString() + "L";
    } else if (type == FieldType.FLOAT.ordinal()) {
      code = new Float(in.readFloat()).toString();
    } else if (type == FieldType.TUPLE.ordinal()){
      readSequence(in, "(", ")");
    } else if (type == FieldType.LIST.ordinal()){
      readSequence(in, "[", "]");
    } else {
      code = WritableUtils.readString(in);
    }
  }

  private void readSequence(DataInput in, String begin, String end) throws IOException {
    int length = WritableUtils.readVInt(in);
    StringBuffer buf = new StringBuffer(begin);
    for (int i = 0; i < length-1; i++) {
      CodeWritable cw = new CodeWritable();
      cw.readFields(in);
      buf.append(cw.get());
      buf.append(",");
    }
    CodeWritable cw = new CodeWritable();
    cw.readFields(in);
    buf.append(cw.get());
    buf.append(end);
    code = buf.toString();
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
