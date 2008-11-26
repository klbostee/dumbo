package org.apache.hadoop.dumbo;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.streaming.PipeMapper;

public class CodeWritablePipeMapper extends PipeMapper {

	@Override
	public void configure(JobConf job) {
		String streamprocessor = job.get("dumbo.code.writable.map.streamprocessor");
		try {
			job.set("stream.map.streamprocessor", URLEncoder.encode(streamprocessor, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		super.configure(job);
	}
	
	@Override
	public void map(Object key, Object value, OutputCollector output, Reporter reporter) throws IOException {
		super.map(key, value, new CodeWritableOutputCollector(output), reporter);
	}
	
	private static class CodeWritableOutputCollector implements OutputCollector {

		private OutputCollector realOutputCollector;
		
		public CodeWritableOutputCollector(OutputCollector realOutputCollector) {
			this.realOutputCollector = realOutputCollector;
		}
		
		@SuppressWarnings("unchecked")
		public void collect(Object key, Object value) throws IOException {
			realOutputCollector.collect(new CodeWritable(key.toString()), new CodeWritable(value.toString()));
		}
		
	}
}
