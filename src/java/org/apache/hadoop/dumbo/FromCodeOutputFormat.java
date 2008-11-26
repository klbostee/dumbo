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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * An output format that converts from Dumbo code.
 */
public class FromCodeOutputFormat implements OutputFormat<Text,Text>, JobConfigurable {

	private OutputFormat realOutputFormat = null;
	
	public FromCodeOutputFormat(OutputFormat realOutputFormat) {
		this.realOutputFormat = realOutputFormat;
	}
	
	public FromCodeOutputFormat() {
		this(null);
	}
	
	public void configure(JobConf job) {
		if (realOutputFormat == null) {
			Class<? extends OutputFormat> realOutputFormatClass
				= job.getClass("dumbo.from.code.output.format.class", TextOutputFormat.class, OutputFormat.class);
			try {
				realOutputFormat = realOutputFormatClass.newInstance();
				for (Class interface_ : realOutputFormatClass.getInterfaces()) {
					if (interface_.equals(JobConfigurable.class)) {
						JobConfigurable jc = (JobConfigurable) realOutputFormat;
						jc.configure(job);
						break;
					}
				}
			} catch (InstantiationException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			} catch (IllegalAccessException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
		// explicitely set the mapoutput classes to make sure that the normal output classes can be different:
		job.setMapOutputKeyClass(job.getMapOutputKeyClass());
		job.setMapOutputValueClass(job.getMapOutputValueClass());
		job.setOutputKeyClass(CodeWritable.class);
		job.setOutputValueClass(CodeWritable.class);
	}
	
	
	public void checkOutputSpecs(FileSystem ignored, JobConf job)
			throws IOException {
		realOutputFormat.checkOutputSpecs(ignored, job);
	}

	@SuppressWarnings("unchecked")
	private RecordWriter<Text,Text> createRecordWriter(FileSystem ignored, JobConf job,
			String name, Progressable progress) throws IOException {
		return new FromCodeRecordWriter(realOutputFormat.getRecordWriter(ignored, job, name, progress));
	}
	
	public RecordWriter<Text,Text> getRecordWriter(FileSystem ignored, JobConf job,
			String name, Progressable progress) throws IOException {
		return createRecordWriter(ignored, job, name, progress);
	}

}
