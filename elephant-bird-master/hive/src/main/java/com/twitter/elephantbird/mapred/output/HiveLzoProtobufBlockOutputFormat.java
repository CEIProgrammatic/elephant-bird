package com.twitter.elephantbird.mapred.output;

import java.io.IOException;
import java.util.Properties;

import com.shaded.google.protobuf.Message;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.mapreduce.output.LzoOutputFormat;
import com.twitter.elephantbird.mapreduce.output.LzoProtobufBlockOutputFormat;

/**
 * Output format class for Hive tables 
 * 
 * To use in data manipulation queries (DML), call 'SET hive.output.file.extension=.lzo;' beforehand.
 * Don't forget to clear the extension afterwards ('SET hive.output.file.extension=;')
 */
public class HiveLzoProtobufBlockOutputFormat<M extends Message> extends DeprecatedLzoProtobufBlockOutputFormat<M> 
	implements HiveOutputFormat<NullWritable, ProtobufWritable<M>> {

  @Override
  public RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
	    Class<? extends Writable> valueClass, boolean isCompressed, 
        Properties tableProperties, Progressable progress) throws IOException {
    
    // let writer know what protobuf class to write with
    String protoClassName = tableProperties.getProperty(serdeConstants.SERIALIZATION_CLASS);
    Class<?> cls = null;
    try {
      if (jc == null) {
        cls = Class.forName(protoClassName);
      } else {
        cls = jc.getClassByName(protoClassName);
      }
      Class<? extends Message> protobufClass = cls.asSubclass(Message.class);
      ((LzoProtobufBlockOutputFormat)this.realOutputFormat).setClassConf(protobufClass, jc);
      DeprecatedOutputFormatWrapper.setOutputFormat(LzoProtobufBlockOutputFormat.class, jc);
    } catch(Exception e) {
      throw new IOException();
    }
    
    jc.set(LzoOutputFormat.CONF_FINAL_OUT_PATH, finalOutPath.toString());
   
    final org.apache.hadoop.mapred.RecordWriter<NullWritable, ProtobufWritable<M>> recordWriter = this.getRecordWriter(null, jc, "dummy", progress);
    // delegate writing & closing to the actual record writer 
    return new RecordWriter() {
      public void write(Writable r) throws IOException {
        recordWriter.write(NullWritable.get(), (ProtobufWritable<M>)r);
      }
      
      public void close(boolean abort) throws IOException {
        recordWriter.close(null);
      }
    };
	}
}
