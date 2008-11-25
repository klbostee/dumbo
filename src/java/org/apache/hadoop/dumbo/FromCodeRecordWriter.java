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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

/**
 * A record writer that converts from Dumbo code.
 */
public class FromCodeRecordWriter implements RecordWriter<Text, Text> {

	private RecordWriter<Writable, Writable> realRecordWriter;
	
	public FromCodeRecordWriter(RecordWriter<Writable, Writable> realRecordWriter) {
		this.realRecordWriter = realRecordWriter;
	}

	public void close(Reporter reporter) throws IOException {
		realRecordWriter.close(reporter);
	}

	public void write(Text key, Text value) throws IOException {
		Writable convertedKey = CodeUtils.codeToWritable(key.toString());
		Writable convertedValue = CodeUtils.codeToWritable(value.toString());
		realRecordWriter.write(convertedKey, convertedValue);
	}

}
