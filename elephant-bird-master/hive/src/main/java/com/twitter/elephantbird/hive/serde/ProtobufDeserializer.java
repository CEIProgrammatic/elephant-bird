package com.twitter.elephantbird.hive.serde;

import com.shaded.google.protobuf.Message;
import com.shaded.google.protobuf.Message.Builder;
import com.shaded.google.protobuf.Descriptors.Descriptor;
import com.twitter.elephantbird.mapreduce.io.ProtobufConverter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;

import java.util.List;
import java.util.Properties;

/**
 * A deserializer for protobufs. Expects protbuf serialized bytes as
 * BytesWritables from input format. <p>
 *
 * Usage: <pre>
 *  create table users
 *    row format serde "com.twitter.elephantbird.hive.serde.ProtobufDeserializer"
 *    with serdeproperties (
 *        "serialization.class"="com.example.proto.gen.Storage$User")
 *    stored as
 *    inputformat "com.twitter.elephantbird.mapred.input.DeprecatedRawMultiInputFormat"
  *  ;
 * </pre>
 */
public class ProtobufDeserializer<M extends Message> implements SerDe {

  private ProtobufConverter<? extends Message> protobufConverter = null;
  private ObjectInspector objectInspector;
  private ProtobufWritable<? extends Message> protobufWritable = null;
  private TypeRef<? extends Message> typeref = null;

  @Override
  public void initialize(Configuration job, Properties tbl) throws SerDeException {
    try {
      String protoClassName = tbl
          .getProperty(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_CLASS);
 
      Class<?> cls = null;
      if (job == null) {
        cls = Class.forName(protoClassName);
      } else {
        cls = job.getClassByName(protoClassName);
      }
      Class<? extends Message> protobufClass = cls.asSubclass(Message.class);
      typeref = new TypeRef<M>(protobufClass){};
      protobufConverter = ProtobufConverter.newInstance(protobufClass);

      Descriptor descriptor = Protobufs.getMessageDescriptor(protobufClass);
      objectInspector = new ProtobufStructObjectInspector(descriptor);
      protobufWritable = ProtobufWritable.newInstance(protobufClass);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    BytesWritable bytes = (BytesWritable) blob;
    return protobufConverter.fromBytes(bytes.getBytes(), 0, bytes.getLength());
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return objectInspector;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return ProtobufWritable.class;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    // use the supplied StructObjectInspector to convert the given object into a protobuf message
    StructObjectInspector ssoi = (StructObjectInspector)objInspector;
    StructObjectInspector pbsoi = (StructObjectInspector)objectInspector;
    
    List<? extends StructField> srcFields = ssoi.getAllStructFieldRefs();
    List<? extends StructField> dstFields = pbsoi.getAllStructFieldRefs();
    if (srcFields.size() != dstFields.size()) {
      throw new SerDeException("ObjectInspector field count mismatch: " + srcFields.size() + "!=" + dstFields.size());
    }
    
    Builder b = Protobufs.getMessageBuilder(typeref.getRawClass()).getDefaultInstanceForType().newBuilderForType();
    
    // set each field, one by one
    for(int i = 0; i < srcFields.size(); i++) {
      StructField srcField = srcFields.get(i);
      ProtobufStructObjectInspector.ProtobufStructField dstField = (ProtobufStructObjectInspector.ProtobufStructField)dstFields.get(i);
      
      Object val = ssoi.getStructFieldData(obj, srcField);
      try {
        if(val instanceof IntWritable) {
          b.setField(dstField.getFieldDescriptor(), ((IntWritable)val).get());
        } else if(val instanceof LongWritable) {
          b.setField(dstField.getFieldDescriptor(), ((LongWritable)val).get());
        } else if(val instanceof BooleanWritable) {
          b.setField(dstField.getFieldDescriptor(), ((BooleanWritable)val).get());
        } else if(val instanceof FloatWritable) {
          b.setField(dstField.getFieldDescriptor(), ((FloatWritable)val).get());
        } else if(val instanceof DoubleWritable) {
          b.setField(dstField.getFieldDescriptor(), ((DoubleWritable)val).get());
        } else if(val instanceof Text) {
          b.setField(dstField.getFieldDescriptor(), ((Text)val).toString());
        } else {
          b.setField(dstField.getFieldDescriptor(), val);
        }
      } catch (IllegalArgumentException ex) {
        System.err.println("Type mis-match. Val: " + val + ", Class: " + val.getClass());
        throw ex;
      }
    }
    
    // build the final message and put it into a Writable object
    protobufWritable.clear();
    protobufWritable.setMessage(b.build());
    return protobufWritable;
  }
}
