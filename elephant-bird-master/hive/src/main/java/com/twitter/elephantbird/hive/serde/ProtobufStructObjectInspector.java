package com.twitter.elephantbird.hive.serde;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import com.shaded.google.protobuf.DescriptorProtos.DescriptorProto;
import com.shaded.google.protobuf.Descriptors.Descriptor;
import com.shaded.google.protobuf.Descriptors.EnumValueDescriptor;
import com.shaded.google.protobuf.Descriptors.FieldDescriptor;
import com.shaded.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.shaded.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.shaded.google.protobuf.ByteString;
import com.shaded.google.protobuf.DescriptorProtos;
import com.shaded.google.protobuf.DescriptorProtos.DescriptorProto;
import com.shaded.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.shaded.google.protobuf.Descriptors;
import com.shaded.google.protobuf.Descriptors.FileDescriptor;
import com.shaded.google.protobuf.Message;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public final class ProtobufStructObjectInspector extends SettableStructObjectInspector implements Externalizable{

  private static void writeProto(ObjectOutput out, DescriptorProtos.FileDescriptorProto proto) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(131072);
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(proto);
    oos.close();
    byte[] serializedProto = baos.toByteArray();
    byte[] bytesToSave = Arrays.copyOf(serializedProto, 131072);
    out.write(bytesToSave);
   }
    
  private static DescriptorProtos.FileDescriptorProto readProto(ObjectInput in) throws IOException, ClassNotFoundException {
    byte[] serializedProto = new byte[131072];
    in.read(serializedProto);
    ByteArrayInputStream bais = new ByteArrayInputStream(serializedProto);
    ObjectInputStream ois = new ObjectInputStream(bais);
    DescriptorProtos.FileDescriptorProto proto = (DescriptorProtos.FileDescriptorProto)ois.readObject();
    ois.close();
    return proto;
  }

  private static List<Integer> getMessageLocator(Descriptor messageType) {
    List<Integer> messageLocator = new ArrayList();
    while (messageType != null) {
        int messageIndex = messageType.getIndex();
        messageLocator.add(0, messageIndex);
        messageType = messageType.getContainingType();
    }
    return messageLocator;
  }

  private static Descriptor getMessage(FileDescriptor fd, List<Integer> messageLocator) {
    int messageIdx = messageLocator.get(0);
    Descriptor message = fd.getMessageTypes().get(messageIdx);
    for (int i=1; i < messageLocator.size(); i++) {
      message = message.getNestedTypes().get(messageLocator.get(i));
    }
    return message;
  }

  public static class ProtobufStructField implements StructField, Externalizable {

    private ObjectInspector oi = null;
    private String comment = null;
    private FieldDescriptor fieldDescriptor;

    public ProtobufStructField() { super(); }
    
    
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        Descriptor containingMessageType = fieldDescriptor.getContainingType();
        DescriptorProtos.FileDescriptorProto proto = containingMessageType.getFile().toProto();
        int fieldIndex = fieldDescriptor.getIndex();
        List<Integer> messageLocator = getMessageLocator(containingMessageType);
        out.writeInt(fieldIndex);
        out.writeObject(messageLocator);
        writeProto(out, proto); 
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int fieldIdx = in.readInt();
        List<Integer> messageLocator = (List<Integer>) in.readObject();
        DescriptorProtos.FileDescriptorProto proto = readProto(in);
        FileDescriptor fd = null;
        try {
            fd = FileDescriptor.buildFrom(proto, new FileDescriptor[]{});
            
        } catch (Descriptors.DescriptorValidationException ex) {
            throw new IOException(ex);
        }
        Descriptor message = getMessage(fd, messageLocator);
        fieldDescriptor = message.getFields().get(fieldIdx);
        oi = this.createOIForField();
    }
    
    
    @SuppressWarnings("unchecked")
    public ProtobufStructField(FieldDescriptor fieldDescriptor) {
      this.fieldDescriptor = fieldDescriptor;
      oi = this.createOIForField();
    }

    @Override
    public String getFieldName() {
      return fieldDescriptor.getName();
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return oi;
    }

    @Override
    public String getFieldComment() {
      return comment;
    }

    public FieldDescriptor getFieldDescriptor() {
      return fieldDescriptor;
    }

    private PrimitiveCategory getPrimitiveCategory(JavaType fieldType) {
      switch (fieldType) {
        case INT:
          return PrimitiveCategory.INT;
        case LONG:
          return PrimitiveCategory.LONG;
        case FLOAT:
          return PrimitiveCategory.FLOAT;
        case DOUBLE:
          return PrimitiveCategory.DOUBLE;
        case BOOLEAN:
          return PrimitiveCategory.BOOLEAN;
        case STRING:
          return PrimitiveCategory.STRING;
        case BYTE_STRING:
          return PrimitiveCategory.BINARY;
        case ENUM:
          return PrimitiveCategory.STRING;
        default:
          return null;
      }
    }

    private ObjectInspector createOIForField() {
      JavaType fieldType = fieldDescriptor.getJavaType();
      PrimitiveCategory category = getPrimitiveCategory(fieldType);
      ObjectInspector elementOI = null;
      if (category != null) {
        elementOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(category);
      } else {
        switch (fieldType) {
          case MESSAGE:
            elementOI = new ProtobufStructObjectInspector(fieldDescriptor.getMessageType());
            break;
          default:
            throw new RuntimeException("JavaType " + fieldType
                + " from protobuf is not supported.");
        }
      }
      if (fieldDescriptor.isRepeated()) {
        return ObjectInspectorFactory.getStandardListObjectInspector(elementOI);
      } else {
        return elementOI;
      }
    }
  }

  private Descriptor descriptor;
  private List<StructField> structFields = Lists.newArrayList();

  
  public ProtobufStructObjectInspector() { super(); }
  
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        DescriptorProtos.FileDescriptorProto proto = descriptor.getFile().toProto();
        List<Integer> messageLocator = getMessageLocator(descriptor);
        out.writeObject(messageLocator);
        writeProto(out,proto);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        List<Integer> messageLocator = (List<Integer>) in.readObject();
        DescriptorProtos.FileDescriptorProto proto = readProto(in);
        FileDescriptor fd = null;
        try {
            fd = FileDescriptor.buildFrom(proto, new FileDescriptor[]{});
            
        } catch (Descriptors.DescriptorValidationException ex) {
            throw new IOException(ex);
        }
        descriptor = getMessage(fd, messageLocator);
        populateStructFields();
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 31 * hash + (this.descriptor != null ? this.descriptor.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ProtobufStructObjectInspector other = (ProtobufStructObjectInspector) obj;
        if (this.descriptor != other.descriptor && (this.descriptor == null || !this.descriptor.equals(other.descriptor))) {
            return false;
        }
        return true;
    }
    
    
  
  ProtobufStructObjectInspector(Descriptor descriptor) {
    this.descriptor = descriptor;
    populateStructFields();
  }

  private void populateStructFields() {
      for (FieldDescriptor fd : descriptor.getFields()) {
      structFields.add(new ProtobufStructField(fd));
    }
  }
  
  @Override
  public Category getCategory() {
    return Category.STRUCT;
  }

  @Override
  public String getTypeName() {
    StringBuilder sb = new StringBuilder("struct<");
    boolean first = true;
    for (StructField structField : getAllStructFieldRefs()) {
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      sb.append(structField.getFieldName()).append(":")
          .append(structField.getFieldObjectInspector().getTypeName());
    }
    sb.append(">");
    return sb.toString();
  }

  @Override
  public Object create() {
    return descriptor.toProto().toBuilder().build();
  }

  @Override
  public Object setStructFieldData(Object data, StructField field, Object fieldValue) {
    return ((Message) data)
        .toBuilder()
        // MiB
        //.setField(descriptor.findFieldByName(field.getFieldName()), fieldValue)
        .setField(((DescriptorProto)data).getDescriptor().findFieldByName(field.getFieldName()), fieldValue)        
        .build();
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return structFields;
  }

  @Override
  public Object getStructFieldData(Object data, StructField structField) {
    if (data == null) {
      return null;
    }
    Message m = (Message) data;
    ProtobufStructField psf = (ProtobufStructField) structField;
    FieldDescriptor fieldDescriptor = psf.getFieldDescriptor();
    
    // This is truly ugly
    fieldDescriptor = m.getDescriptorForType().findFieldByName(fieldDescriptor.getName());
    
    Object result = m.getField(fieldDescriptor);
    if (fieldDescriptor.getType() == Type.ENUM) {
      return ((EnumValueDescriptor)result).getName();
    }
    if (fieldDescriptor.getType() == Type.BYTES && (result instanceof ByteString)) {
        return ((ByteString)result).toByteArray();
    }
    return result;
  }

  @Override
  public StructField getStructFieldRef(String fieldName) {
    FieldDescriptor fieldDesc = descriptor.findFieldByName(fieldName);
    if (fieldDesc == null) {
      // CRO 21Jan2014: Hive casing causes confusion; search for descriptor one name at a time
      //  see StandardStructObjectInspector.MyField constructor
      for(FieldDescriptor fd : descriptor.getFields()) {
        if(fd.getName().toLowerCase().equals(fieldName)) {
          //System.err.println("Found a field descriptor: " + fieldName);
          fieldDesc = fd;
          break;
        }
      }
    }
    return new ProtobufStructField(fieldDesc);
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }
    List<Object> result = Lists.newArrayList();
    Message m = (Message) data;
    for (FieldDescriptor fd : descriptor.getFields()) {
      // This is truly ugly
      FieldDescriptor fieldDescriptor = m.getDescriptorForType().findFieldByName(fd.getName());
      result.add(m.getField(fieldDescriptor));
    }
    return result;
  }
}
