/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.thrift.protocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class TReadableJSONProtocol extends TProtocol {

  /**
   * Factory
   */
  public static class Factory implements TProtocolFactory {
    public TProtocol getProtocol(TTransport trans) {
      return new TReadableJSONProtocol(trans);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TReadableJSONProtocol.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final JsonNodeFactory NODE_FACTORY = JsonNodeFactory.instance;

  private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

  private static final TStruct ANONYMOUS_STRUCT = new TStruct();

  protected static byte[] serializeJSON(JsonNode node) throws TProtocolException {
    try {
      final Object obj = OBJECT_MAPPER.treeToValue(node, Object.class);
      return OBJECT_MAPPER.writeValueAsBytes(obj);
    } catch (JsonProcessingException e) {
      throw new TProtocolException(TProtocolException.UNKNOWN,
                                   "Error serializing JSON: " + e.getMessage());
    }
  }

  protected static JsonNode parseJSON(byte[] data) throws TProtocolException {
    try {
      return OBJECT_MAPPER.readTree(data);
    } catch (IOException e) {
      throw new TProtocolException(TProtocolException.INVALID_DATA,
                                   "Error parsing JSON: " + e.getMessage());
    }
  }

  protected abstract class WriterContext {
    private String fieldName_ = null;

    protected String getFieldName() {
      return fieldName_;
    }

    protected void setFieldName(String fieldName) {
      fieldName_ = fieldName;
    }

    protected boolean isMapKey() { return false; }

    abstract JsonNode getJsonNode();

    abstract void write(JsonNode node);
  }

  protected class StructWriterContext extends WriterContext {
    ObjectNode objectNode_ = NODE_FACTORY.objectNode();

    @Override
    protected JsonNode getJsonNode() {
      return objectNode_;
    }

    @Override
    protected void write(JsonNode node) {
      objectNode_.put(getFieldName(), node);
    }
  }

  protected class MapWriterContext extends WriterContext {
    ObjectNode objectNode_ = NODE_FACTORY.objectNode();

    protected boolean isKey_ = true;
    protected String key_ = null;

    @Override
    protected boolean isMapKey() {
      return isKey_;
    }

    @Override
    protected JsonNode getJsonNode() {
      return objectNode_;
    }

    @Override
    protected void write(JsonNode node) {
      if (isKey_) {
        assert node.isTextual();
        key_ = node.asText();
      } else {
        assert key_ != null;
        objectNode_.put(key_, node);
        key_ = null;
      }

      isKey_ = !isKey_;
    }
  }

  protected class ListWriterContext extends WriterContext {
    ArrayNode arrayNode_ = NODE_FACTORY.arrayNode();

    @Override
    protected JsonNode getJsonNode() {
      return arrayNode_;
    }

    @Override
    protected void write(JsonNode node) {
      arrayNode_.add(node);
    }
  }

  protected class SetWriterContext extends ListWriterContext {
  }

  protected abstract class ReaderContext {
    abstract boolean hasNext();

    abstract JsonNode next();

    protected boolean isMapKey() { return false; }
  }

  protected class StructReaderContext extends ReaderContext {
    private JsonNode node_ = null;
    private Iterator<Map.Entry<String, JsonNode>> fieldIterator_ = null;
    private Map.Entry<String, JsonNode> nextField_ = null;

    StructReaderContext(JsonNode node) throws TProtocolException {
      if (!node.isObject()) {
        throw new TProtocolException(TProtocolException.INVALID_DATA,
                                     "Expected JSON node of object type");
      }
      node_ = node;
      fieldIterator_ = node_.fields();

      if (fieldIterator_.hasNext()) {
        nextField_ = fieldIterator_.next();
      }
    }

    protected String peekKey() {
      return nextField_.getKey();
    }

    protected JsonNode peekValue() {
      return nextField_.getValue();
    }

    @Override
    protected boolean hasNext() {
      return nextField_ != null;
    }

    @Override
    protected JsonNode next() {
      JsonNode nextNode = peekValue();

      if (fieldIterator_.hasNext()) {
        nextField_ = fieldIterator_.next();
      } else {
        nextField_ = null;
      }

      return nextNode;
    }
  }

  protected class MapReaderContext extends StructReaderContext {
    private boolean isKey_ = true;

    MapReaderContext(JsonNode node) throws TProtocolException {
      super(node);
    }

    @Override
    protected JsonNode next() {
      isKey_ = !isKey_;
      return isKey_ ? super.next() : OBJECT_MAPPER.convertValue(peekKey(), JsonNode.class);
    }

    @Override
    protected boolean isMapKey() {
      return isKey_;
    }
  }

  protected class ListReaderContext extends ReaderContext {
    private JsonNode node_ = null;
    private Iterator<JsonNode> elementIterator_ = null;

    ListReaderContext(JsonNode node) throws TProtocolException {
      if (!node.isArray()) {
        throw new TProtocolException(TProtocolException.INVALID_DATA,
                                     "Expected JSON node of array type");
      }
      node_ = node;
      elementIterator_ = node_.elements();
    }

    @Override
    protected boolean hasNext() {
      return elementIterator_.hasNext();
    }

    @Override
    protected JsonNode next() {
      return elementIterator_.next();
    }
  }

  protected class SetReaderContext extends ListReaderContext {
    SetReaderContext(JsonNode node) throws TProtocolException {
      super(node);
    }
  }

  abstract class JsonNodeConverter {
    abstract JsonNode fromBool(boolean value) throws TException;
    abstract JsonNode fromByte(byte value) throws TException;
    abstract JsonNode fromI16(short value) throws TException;
    abstract JsonNode fromI32(int value) throws TException;
    abstract JsonNode fromI64(long value) throws TException;
    abstract JsonNode fromDouble(double value) throws TException;
    abstract JsonNode fromString(String str) throws TException;
    abstract JsonNode fromBinary(ByteBuffer data) throws TException;
    abstract JsonNode fromJsonNode(JsonNode node) throws TException;

    abstract boolean toBool(JsonNode node) throws TException;
    abstract byte toByte(JsonNode node) throws TException;
    abstract short toI16(JsonNode node) throws TException;
    abstract int toI32(JsonNode node) throws TException;
    abstract long toI64(JsonNode node) throws TException;
    abstract double toDouble(JsonNode node) throws TException;
    abstract String toString(JsonNode node) throws TException;
    abstract ByteBuffer toBinary(JsonNode node) throws TException;
    abstract JsonNode toJsonNode(JsonNode node) throws TException;
  }

  protected class JsonKeyNodeConverter extends JsonNodeConverter {

    JsonNode fromBool(boolean value) {
      return NODE_FACTORY.textNode(value ? "true" : "false");
    }

    JsonNode fromByte(byte value) {
      return NODE_FACTORY.textNode(Byte.toString(value));
    }

    JsonNode fromI16(short value) {
      return NODE_FACTORY.textNode(Short.toString(value));
    }

    JsonNode fromI32(int value) {
      return NODE_FACTORY.textNode(Integer.toString(value));
    }

    JsonNode fromI64(long value) {
      return NODE_FACTORY.textNode(Long.toString(value));
    }

    JsonNode fromDouble(double value) {
      return NODE_FACTORY.textNode(Double.toString(value));
    }

    JsonNode fromString(String str) {
      return NODE_FACTORY.textNode(str);
    }

    JsonNode fromBinary(ByteBuffer data) {
      return NODE_FACTORY.binaryNode(data.array());
    }

    JsonNode fromJsonNode(JsonNode node) throws TProtocolException {
      return NODE_FACTORY.textNode(new String(serializeJSON(node), UTF8_CHARSET));
    }

    boolean toBool(JsonNode node) throws TException {
      final String str = toString(node);

      if (str.equals("true")) {
        return true;
      } else if (str.equals("false")) {
        return false;
      } else {
        throw new TProtocolException(TProtocolException.INVALID_DATA,
                                     "JSON node is not a bool");
      }
    }

    byte toByte(JsonNode node) throws TException {
      try {
        return Byte.parseByte(toString(node));
      } catch (NumberFormatException e) {
        throw new TProtocolException(TProtocolException.INVALID_DATA,
                                     "JSON node is not a byte");
      }
    }

    short toI16(JsonNode node) throws TException {
      try {
        return Short.parseShort(toString(node));
      } catch (NumberFormatException e) {
        throw new TProtocolException(TProtocolException.INVALID_DATA,
                                     "JSON node is not a short");
      }
    }

    int toI32(JsonNode node) throws TException {
      try {
        return Integer.parseInt(toString(node));
      } catch (NumberFormatException e) {
        throw new TProtocolException(TProtocolException.INVALID_DATA,
                                     "JSON node is not a int");
      }
    }

    long toI64(JsonNode node) throws TException {
      try {
        return Long.parseLong(toString(node));
      } catch (NumberFormatException e) {
        throw new TProtocolException(TProtocolException.INVALID_DATA,
                                     "JSON node is not a long");
      }
    }

    double toDouble(JsonNode node) throws TException {
      try {
        return Double.parseDouble(toString(node));
      } catch (NumberFormatException e) {
        throw new TProtocolException(TProtocolException.INVALID_DATA,
                                     "JSON node is not a double");
      }
    }

    String toString(JsonNode node) throws TException {
      if (!node.isTextual()) {
        throw new TProtocolException(TProtocolException.INVALID_DATA,
                                     "JSON node is not a string");
      }
      return node.asText();
    }

    ByteBuffer toBinary(JsonNode node) throws TException {
      byte[] data;
      try {
        data = node.binaryValue();
      } catch (IOException e) {
        data = null;
      }
      if (data == null) {
        throw new TProtocolException(TProtocolException.INVALID_DATA,
                                     "JSON node is not a base 64 encoded string");
      }
      return ByteBuffer.wrap(data);
    }

    JsonNode toJsonNode(JsonNode node) throws TException {
      return parseJSON(toString(node).getBytes(UTF8_CHARSET));
    }
  }

  protected class JsonValueNodeConverter extends JsonNodeConverter {

    JsonNode fromBool(boolean value) {
      return NODE_FACTORY.booleanNode(value);
    }

    JsonNode fromByte(byte value) {
      return NODE_FACTORY.numberNode(value);
    }

    JsonNode fromI16(short value) {
      return NODE_FACTORY.numberNode(value);
    }

    JsonNode fromI32(int value) {
      return NODE_FACTORY.numberNode(value);
    }

    JsonNode fromI64(long value) {
      return NODE_FACTORY.numberNode(value);
    }

    JsonNode fromDouble(double value) {
      return NODE_FACTORY.numberNode(value);
    }

    JsonNode fromString(String str) {
      return NODE_FACTORY.textNode(str);
    }

    JsonNode fromBinary(ByteBuffer data) {
      return NODE_FACTORY.binaryNode(data.array());
    }

    JsonNode fromJsonNode(JsonNode node) {
      return node;
    }

    boolean toBool(JsonNode node) throws TException {
      if (!node.isBoolean()) {
        throw new TProtocolException(TProtocolException.INVALID_DATA,
                                     "JSON node is not a bool");
      }
      return node.asBoolean();
    }

    byte toByte(JsonNode node) throws TException {
      if (!node.isIntegralNumber() || !node.canConvertToInt()) {
        throw new TProtocolException(TProtocolException.INVALID_DATA,
                                     "JSON node is not a byte");
      }
      // TODO(ahilss): range check.
      return (byte) node.asInt();
    }

    short toI16(JsonNode node) throws TException {
      if (!node.isIntegralNumber() || !node.canConvertToInt()) {
        throw new TProtocolException(TProtocolException.INVALID_DATA,
                                     "JSON node is not a short");
      }
      // TODO(ahilss): range check.
      return (short) node.asInt();
    }

    int toI32(JsonNode node) throws TException {
      if (!node.isIntegralNumber() || !node.canConvertToInt()) {
        throw new TProtocolException(TProtocolException.INVALID_DATA,
                                     "JSON node is not an int");
      }
      return node.asInt();
    }

    long toI64(JsonNode node) throws TException {
      if (!node.isIntegralNumber() || !node.canConvertToLong()) {
        throw new TProtocolException(TProtocolException.INVALID_DATA,
                                     "JSON node is not a long");
      }
      return node.asLong();
    }

    double toDouble(JsonNode node) throws TException {
      // TODO(ahilss): strictly enforce using isFloatingPointNumber?
      if (!node.isNumber()) {
        throw new TProtocolException(TProtocolException.INVALID_DATA,
                                     "JSON node is not a double");
      }
      return node.asDouble();
    }

    String toString(JsonNode node) throws TException {
      if (!node.isTextual()) {
        throw new TProtocolException(TProtocolException.INVALID_DATA,
                                     "JSON node is not a string");
      }
      return node.asText();
    }

    ByteBuffer toBinary(JsonNode node) throws TException {
      byte[] data;
      try {
        data = node.binaryValue();
      } catch (IOException e) {
        data = null;
      }
      if (data == null) {
        throw new TProtocolException(TProtocolException.INVALID_DATA,
                                     "JSON node is not a base 64 encoded string");
      }
      return ByteBuffer.wrap(data);
    }

    JsonNode toJsonNode(JsonNode node) throws TException {
      return node;
    }
  }

  protected JsonKeyNodeConverter keyConverter = new JsonKeyNodeConverter();
  protected JsonValueNodeConverter valueConverter = new JsonValueNodeConverter();

  protected JsonNodeConverter getWriterConverter() {
    return topWriterContext().isMapKey() ? keyConverter : valueConverter;
  }

  protected JsonNodeConverter getReaderConverter() {
    return topReaderContext().isMapKey() ? keyConverter : valueConverter;
  }

  protected Stack<WriterContext> writerContextStack_ = new Stack<WriterContext>();

  protected void pushWriterContext(WriterContext context) {
    writerContextStack_.push(context);
  }

  protected void popWriterContext() throws TException {
    JsonNode node = topWriterContext().getJsonNode();
    writerContextStack_.pop();
    writeNode(getWriterConverter().fromJsonNode(node));
  }

  protected WriterContext topWriterContext() {
    return writerContextStack_.peek();
  }

  protected Stack<ReaderContext> readerContextStack_ = new Stack<ReaderContext>();

  protected void pushReaderContext(ReaderContext context) {
    readerContextStack_.push(context);
  }

  protected void popReaderContext() {
    readerContextStack_.pop();
  }

  protected ReaderContext topReaderContext() {
    return readerContextStack_.peek();
  }

  protected void writeNode(JsonNode node) throws TException {
    if (writerContextStack_.size() == 1) {
      getTransport().write(serializeJSON(node));
    } else {
      topWriterContext().write(node);
    }
  }

  protected JsonNode readNode() {
    return topReaderContext().next();
  }

  protected byte[] readAllDataFromTransport() throws TTransportException {
    final int READ_BUFFER_SIZE = 1024;
    final int MAX_READ_SIZE = 1 << 30;
    final byte[] readBuffer = new byte[READ_BUFFER_SIZE];

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final TTransport transport = getTransport();

    boolean hasMoreData = true;

    while (hasMoreData && out.size() < MAX_READ_SIZE) {
      int numRead = transport.read(readBuffer, 0, READ_BUFFER_SIZE);
      out.write(readBuffer, 0, numRead);

      if (numRead < READ_BUFFER_SIZE) {
        hasMoreData = false;
      }
    }

    return out.toByteArray();
  }

  public TReadableJSONProtocol(TTransport trans) {
    super(trans);
    pushWriterContext(new StructWriterContext());
  }

  public void writeMessageBegin(TMessage message) throws TException {
    throw new TProtocolException(TProtocolException.NOT_IMPLEMENTED,
                                 "writeMessageBegin not implemented");
  }

  public void writeMessageEnd() throws TException {
    throw new TProtocolException(TProtocolException.NOT_IMPLEMENTED,
                                 "writeMessageEnd not implemented");
  }

  public void writeStructBegin(TStruct struct) throws TException {
    LOGGER.debug("writeStructBegin");
    pushWriterContext(new StructWriterContext());
  }

  public void writeStructEnd() throws TException {
    LOGGER.debug("writeStructEnd");
    popWriterContext();
  }

  public void writeFieldBegin(TField field) throws TException {
    LOGGER.debug("writeFieldBegin: {}", field.name);
    StructWriterContext writerContext = (StructWriterContext) topWriterContext();
    writerContext.setFieldName(field.name);
  }

  public void writeFieldEnd() {}

  public void writeFieldStop() {}

  public void writeMapBegin(TMap map) throws TException {
    LOGGER.debug("writeMapBegin");
    pushWriterContext(new MapWriterContext());
  }

  public void writeMapEnd() throws TException {
    LOGGER.debug("writeMapEnd");
    popWriterContext();
  }

  public void writeListBegin(TList list) throws TException {
    LOGGER.debug("writeListBegin");
    pushWriterContext(new ListWriterContext());
  }

  public void writeListEnd() throws TException {
    LOGGER.debug("writeListEnd");
    popWriterContext();
  }

  public void writeSetBegin(TSet set) throws TException {
    LOGGER.debug("writeSetBegin");
    pushWriterContext(new ListWriterContext());
  }

  public void writeSetEnd() throws TException {
    LOGGER.debug("writeSetEnd");
    popWriterContext();
  }

  public void writeBool(boolean b) throws TException {
    LOGGER.debug("writeBool");
    writeNode(getWriterConverter().fromBool(b));
  }

  public void writeByte(byte b) throws TException {
    LOGGER.debug("writeByte");
    writeNode(getWriterConverter().fromByte(b));
  }

  public void writeI16(short i16) throws TException {
    LOGGER.debug("writeI16");
    writeNode(getWriterConverter().fromI16(i16));
  }

  public void writeI32(int i32) throws TException {
    LOGGER.debug("writeI32");
    writeNode(getWriterConverter().fromI32(i32));
  }

  public void writeI64(long i64) throws TException {
    LOGGER.debug("writeI64");
    writeNode(getWriterConverter().fromI64(i64));
  }

  public void writeDouble(double dub) throws TException {
    LOGGER.debug("writeDouble");
    writeNode(getWriterConverter().fromDouble(dub));
  }

  public void writeString(String str) throws TException {
    LOGGER.debug("writeString");
    writeNode(getWriterConverter().fromString(str));
  }

  public void writeBinary(ByteBuffer bin) throws TException {
    LOGGER.debug("writeBinary");
    writeNode(getWriterConverter().fromBinary(bin));
  }

  public TMessage readMessageBegin() throws TException {
    throw new TProtocolException(TProtocolException.NOT_IMPLEMENTED,
                                 "readMessageBegin not implemented");
  }

  public void readMessageEnd() throws TException {
    throw new TProtocolException(TProtocolException.NOT_IMPLEMENTED,
                                 "readMessageEnd not implemented");
  }

  public TStruct readStructBegin() throws TException {
    LOGGER.debug("readStructBegin");

    JsonNode node = null;

    if (readerContextStack_.empty()) {
      node = parseJSON(readAllDataFromTransport());
    } else {
      node = getReaderConverter().toJsonNode(readNode());
    }

    pushReaderContext(new StructReaderContext(node));

    return ANONYMOUS_STRUCT;
  }

  public void readStructEnd() {
    LOGGER.debug("readStructEnd");
    popReaderContext();
  }

  public TField readFieldBegin() throws TException {
    StructReaderContext readerContext = (StructReaderContext) topReaderContext();

    String name = "";
    byte type = TType.UNKNOWN;
    short id = TField.FIELD_ID_UNKNOWN;

    if (readerContext.hasNext()) {
      if (!readerContext.peekValue().isNull()) {
        name = readerContext.peekKey();
      }
    } else {
      type = TType.STOP;
    }

    LOGGER.debug("readFieldBegin: {}", name);

    return new TField(name, type, id);
  }

  public void readFieldEnd() {}

  public TMap readMapBegin() throws TException {
    LOGGER.debug("readMapBegin");
    final JsonNode node = getReaderConverter().toJsonNode(readNode());
    pushReaderContext(new MapReaderContext(node));
    return new TMap(TType.UNKNOWN, TType.UNKNOWN, node.size());
  }

  public void readMapEnd() {
    LOGGER.debug("readMapEnd");
    popReaderContext();
  }

  public TList readListBegin() throws TException {
    LOGGER.debug("readListBegin");
    final JsonNode node = getReaderConverter().toJsonNode(readNode());
    pushReaderContext(new ListReaderContext(node));
    return new TList(TType.UNKNOWN, node.size());
  }

  public void readListEnd() {
    LOGGER.debug("readListEnd");
    popReaderContext();
  }

  public TSet readSetBegin() throws TException {
    LOGGER.debug("readSetBegin");
    final JsonNode node = getReaderConverter().toJsonNode(readNode());
    pushReaderContext(new SetReaderContext(node));
    return new TSet(TType.UNKNOWN, node.size());
  }

  public void readSetEnd() {
    LOGGER.debug("readSetEnd");
    popReaderContext();
  }

  public boolean readBool() throws TException {
    LOGGER.debug("readBool");
    return getReaderConverter().toBool(readNode());
  }

  public byte readByte() throws TException {
    LOGGER.debug("readByte");
    return getReaderConverter().toByte(readNode());
  }

  public short readI16() throws TException {
    LOGGER.debug("readI16");
    return getReaderConverter().toI16(readNode());
  }

  public int readI32() throws TException {
    LOGGER.debug("readI32");
    return getReaderConverter().toI32(readNode());
  }

  public long readI64() throws TException {
    LOGGER.debug("readI64");
    return getReaderConverter().toI64(readNode());
  }

  public double readDouble() throws TException {
    LOGGER.debug("readDouble");
    return getReaderConverter().toDouble(readNode());
  }

  public String readString() throws TException {
    LOGGER.debug("readString");
    return getReaderConverter().toString(readNode());
  }

  public ByteBuffer readBinary() throws TException {
    LOGGER.debug("readBinary");
    return getReaderConverter().toBinary(readNode());
  }

  public void readUnknown() {
    readNode();
  }

  public static class CollectionMapKeyException extends TException {
    public CollectionMapKeyException(String message) {
      super(message);
    }
  }
}
