package org.apache.hadoop.dumbo;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CodeWritableMapper implements Mapper {

  private Mapper realMapper = null;

  public void configure(JobConf job) {
    try {
      realMapper = job.getClass("dumbo.code.writable.map.class", null, Mapper.class).newInstance();
    } catch (InstantiationException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    if (realMapper == null) throw new RuntimeException("map class property not set");
    realMapper.configure(job);
  }

  @SuppressWarnings("unchecked")
  public void map(Object key, Object value, OutputCollector output, Reporter reporter) throws IOException {
    realMapper.map(key, value, new CodeWritableOutputCollector(output), reporter);
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

  public void close() throws IOException {
    realMapper.close();
  }
}
