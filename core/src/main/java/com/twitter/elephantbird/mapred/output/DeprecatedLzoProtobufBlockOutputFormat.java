package com.twitter.elephantbird.mapred.output;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.output.LzoProtobufBlockOutputFormat;
import org.apache.hadoop.io.NullWritable;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.TypeRef;

/**
 * mapred version of {@link LzoProtobufBlockOutputFormat}.
 */
public class DeprecatedLzoProtobufBlockOutputFormat<M extends Message>
               extends DeprecatedOutputFormatWrapper<NullWritable, ProtobufWritable<M>> {

  public DeprecatedLzoProtobufBlockOutputFormat() {
    super(new LzoProtobufBlockOutputFormat());
  }
  
  public DeprecatedLzoProtobufBlockOutputFormat(TypeRef<M> typeRef) {
    super(new LzoProtobufBlockOutputFormat(typeRef));
  }
}
