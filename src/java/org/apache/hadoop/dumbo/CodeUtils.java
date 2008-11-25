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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.record.Record;

/**
 * This class provides functions that generate and parse Dumbo code.
 */
public abstract class CodeUtils {
	
	public static enum CodeType {
    NULL, BOOLEAN, INTEGER, LONG, FLOAT, STRING, TUPLE, LIST, DICTIONARY
  }

  private CodeUtils() {}

  public static final String NULL_CODE = "None";

  public static String booleanToCode(boolean b) {
    return b ? "True" : "False";
  }
  
  public static boolean codeToBoolean(String code) {
  	return code.charAt(0) == 'T';
  }
  
  public static String intToCode(int i) {
  	return numberToCode(i);
  }
  
  public static String longToCode(long l) {
  	return numberToCode(l);
  }
  
  public static String floatToCode(float f) {
  	return numberToCode(f);
  }
  
  public static String doubleToCode(double d) {
  	return numberToCode(d);
  }
  
  private static String numberToCode(Number n) {
    return n.toString();
  }
  
  public static int codeToInt(String code) {
  	return Integer.parseInt(code);
  }
  
  public static long codeToLong(String code) {
  	return Long.parseLong(code.substring(0, code.length()-1));
  }
  
  public static float codeToFloat(String code) {
  	return Float.parseFloat(code);
  }

  public static String stringToCode(String s) {
    return "'" + s.replace("\\", "\\\\")
    .replace("\n", "\\n")
    .replace("\r", "\\r")
    .replace("\t", "\\t")
    .replace("'", "\\'")
    + "'";
  }
  
  public static String codeToString(String code) {
  	return code.substring(1, code.length()-1)
  	.replace("\\n", "\n")
  	.replace("\\r", "\r")
  	.replace("\\t", "\t")
  	.replace("\\'", "'");
  }

  
  public static String codesToTuple(String[] codes) {
    return combineSubcodes(codes, "(", ")");
  }
  
  public static String codesToTuple(String code1, String code2) {
    return "(" + code1 + "," + code2 + ")";
  }
  
  public static String codesToList(String[] codes) {
    return combineSubcodes(codes, "[", "]");
  }
  
  public static String codesToList(String code1, String code2) {
    return "[" + code1 + "," + code2 + "]";
  }
  
  private static String combineSubcodes(String[] codes, String begin, String end) {
    StringBuffer buf = new StringBuffer(begin);
    for (int i = 0; i < codes.length-1; i++) {
      buf.append(codes[i]);
      buf.append(",");
    }
    buf.append(codes[codes.length-1]);
    buf.append(end);
    return buf.toString();
  }
  
  public static String[] codesFromTuple(String code) {
  	return findSubcodes(code);
  }
  
  public static String[] codesFromList(String code) {
  	return findSubcodes(code);
  }
  
  private static String[] findSubcodes(String code) {
  	List<String> codes = new ArrayList<String>();
    boolean inStr = false;
    int prevIndex = 1; 
    for (int i = 1; i < code.length()-1; i++) {
      char c = code.charAt(i);
      if (c == '\'' || c == '"') inStr = !inStr;
      else if (!inStr && c == ',') {
        codes.add(code.substring(prevIndex, i).trim());
        prevIndex = i+1;
      }
    }
    codes.add(code.substring(prevIndex, code.length()-1).trim());
    String[] codesArray = new String[codes.size()];
    codes.toArray(codesArray);
    return codesArray;
  }
  
  
  public static CodeType deriveType(String code) {
  	char first = code.charAt(0);
    if (code.equals(NULL_CODE)) {
      return CodeType.NULL;
    } else if (code.equals("True") || code.equals("False")) {
      return CodeType.BOOLEAN;
    } else if (first == '\'' || first == '"') {
      return CodeType.STRING;
    } else if (first == '(') {
      return CodeType.TUPLE;
    } else if (first == '[') {
      return CodeType.LIST;
    } else if (first == '{') {
      return CodeType.DICTIONARY;
    } else if (code.contains(".")) {
      return CodeType.FLOAT;
    } else if (code.charAt(code.length()-1) == 'L') {
      return CodeType.LONG;
    } else {
      return CodeType.INTEGER;
    }
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
  	if (w instanceof BooleanWritable) {
  		return booleanToCode(((BooleanWritable)w).get());
  	} else if (w instanceof IntWritable)  {
  		return intToCode(((IntWritable)w).get());
  	} else if (w instanceof VIntWritable)  {
  		return intToCode(((VIntWritable)w).get());
  	} else if (w instanceof LongWritable) {
  		return longToCode(((LongWritable)w).get());
   	} else if (w instanceof VLongWritable) {
  		return longToCode(((VLongWritable)w).get());
  	} else if (w instanceof FloatWritable) {
  		return floatToCode(((FloatWritable)w).get());
  	} else if (w instanceof Text) {
  		return stringToCode(((Text)w).toString());
  	} else if (w instanceof Record) {
  		return recordToCode((Record)w);
  	} else if (w instanceof CodeWritable) {
  		return ((CodeWritable)w).get();
  	} else return stringToCode(w.toString());
  }

  public static Writable codeToWritable(String code) {
  	CodeType type = deriveType(code);
  	if (type == CodeType.BOOLEAN) {
  		return new BooleanWritable(codeToBoolean(code));
  	} else if (type == CodeType.INTEGER) {
  		return new VIntWritable(codeToInt(code));
  	} else if (type == CodeType.LONG) {
  		return new VLongWritable(codeToLong(code));
  	} else if (type == CodeType.FLOAT) {
  		return new FloatWritable(codeToFloat(code));
  	} else if (type == CodeType.STRING) {
  		return new Text(codeToString(code));
  	} else return new CodeWritable(code);
  }
  
}
