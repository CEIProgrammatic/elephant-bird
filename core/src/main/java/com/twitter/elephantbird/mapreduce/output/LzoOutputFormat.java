package com.twitter.elephantbird.mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.util.LzoUtils;

/**
 * Base class for Lzo outputformats.
 * provides an helper method to create lzo output stream.
 */
public abstract class LzoOutputFormat<K, V> extends WorkFileOverride.FileOutputFormat<K, V> {

  public static final Logger LOG = LoggerFactory.getLogger(LzoOutputFormat.class);
  public static final String CONF_FINAL_OUT_PATH = "HiveRecordWriter.finalOutPath";

  /**
   * Helper method to create lzo output file needed to create RecordWriter
   */
  protected DataOutputStream getOutputStream(TaskAttemptContext job)
                  throws IOException, InterruptedException {

    Path workFile = null;
    try {
      workFile = getDefaultWorkFile(job, LzopCodec.DEFAULT_LZO_EXTENSION);
    } catch (Exception e) {
      // see com.twitter.elephantbird.mapred.output.HiveLzoProtobufBlockOutputFormat
      workFile = new Path(job.getConfiguration().get(CONF_FINAL_OUT_PATH));
    }
    
    return LzoUtils.getIndexedLzoOutputStream(
                      HadoopCompat.getConfiguration(job),
                      workFile);
  }
}
