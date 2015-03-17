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

#import <Foundation/NSJSONSerialization.h>

#import "TSimpleJSONProtocol.h"
#import "TProtocolException.h"
#import "Thrift.h"
#import "TObjective-C.h"


@interface TReaderContext : NSObject {
}

- (BOOL) isAtEnd;
- (NSObject *) topObject;
- (NSObject *) nextObject;
- (void) advance;
- (BOOL) isKeyContext;

@end

@implementation TReaderContext

- (BOOL) isAtEnd
{
  @throw [TProtocolException exceptionWithName:@"TProtocolException"
              reason:@"Attempt to call abstract base class method"];
  return YES;
}

- (NSObject *) topObject
{
  @throw [TProtocolException exceptionWithName:@"TProtocolException"
              reason:@"Attempt to call abstract base class method"];

}

- (NSObject *) nextObject
{
  NSObject *obj = [self topObject];
  if (![self isAtEnd]) {
    [self advance];
  }
  return obj;
}

- (void) advance
{
  @throw [TProtocolException exceptionWithName:@"TProtocolException"
              reason:@"Attempt to call abstract base class method"];
}

- (BOOL) isKeyContext
{
  return NO;
}

@end

@interface TStructReaderContext : TReaderContext {
  NSDictionary *_dictionary;
  NSEnumerator *_keyEnumerator;
  NSString *_nextKey;
  NSObject *_nextValue;
}

- (id) initWithDictionary:(NSDictionary *)dictionary;

@property (nonatomic, retain, readonly) NSDictionary* dictionary;
@property (nonatomic, retain) NSEnumerator* keyEnumerator;
@property (nonatomic, retain) NSString* nextKey;
@property (nonatomic, retain) NSObject* nextValue;

@end

@implementation TStructReaderContext

- (id) initWithDictionary:(NSDictionary *)dictionary
{
  self = [super init];

  if (self) {
    _dictionary = [dictionary retain_stub];
    _keyEnumerator = [dictionary keyEnumerator];

    [self advance];
  }

  return self;
}

- (void) dealloc
{
  [_dictionary release_stub];
  [super dealloc_stub];
}

- (BOOL) isAtEnd
{
  return !self.nextKey;
}

- (NSObject *) topObject
{
  return self.nextValue;
}

- (void) advance
{
  id nextObject = [self.keyEnumerator nextObject];
  if (nextObject) {
    if (![nextObject isKindOfClass:[NSString class]]) {
      @throw [TProtocolException exceptionWithName:@"TProtocolException"
                                            reason:@"Object keys must be strings"];
    }
    self.nextKey = (NSString *) nextObject;
    self.nextValue = [self.dictionary objectForKey:self.nextKey];
  } else {
    self.nextKey = nil;
    self.nextValue = nil;
  }
}

@end

typedef enum {
  KEY_READER_STATE,
  VALUE_READER_STATE
} TMapReaderState;

@interface TMapReaderContext : TReaderContext {
  NSDictionary *_dictionary;
  NSEnumerator *_keyEnumerator;
  NSObject *_nextValue;
  TMapReaderState _state;
}

- (id) initWithDictionary:(NSDictionary *)dictionary;

@property (nonatomic, retain, readonly) NSDictionary* dictionary;
@property (nonatomic, retain) NSEnumerator* keyEnumerator;
@property (nonatomic, retain) NSObject* nextValue;
@property (nonatomic, assign) TMapReaderState state;

@end

@implementation TMapReaderContext

- (id) initWithDictionary:(NSDictionary *)dictionary
{
  self = [super init];

  if (self) {
    _dictionary = [dictionary retain_stub];
    _keyEnumerator = [dictionary keyEnumerator];
    _state = VALUE_READER_STATE;

    [self advance];
  }

  return self;
}

- (void) dealloc
{
  [_dictionary release_stub];
  [super dealloc_stub];
}

- (BOOL) isAtEnd
{
  return !self.nextValue;
}

- (NSObject *) topObject
{
  return self.nextValue;
}

- (void) advance
{
  if (self.state == KEY_READER_STATE) {
    if (self.nextValue) {
      self.nextValue = [self.dictionary objectForKey:self.nextValue];
      self.state = VALUE_READER_STATE;
    }
  } else { /* VALUE_STATE */
    self.nextValue = [self.keyEnumerator nextObject];
    self.state = KEY_READER_STATE;

    if (self.nextValue && ![self.nextValue isKindOfClass:[NSString class]]) {
      @throw [TProtocolException exceptionWithName:@"TProtocolException"
                                            reason:@"Map keys must be strings"];
    }
  }
}

- (BOOL) isKeyContext
{
  return self.state == KEY_READER_STATE;
}

@end

@interface TListReaderContext : TReaderContext {
  NSArray *_array;
  NSUInteger _index;
}

- (id) initWithArray:(NSArray *)array;

@property (nonatomic, retain, readonly) NSArray* array;
@property (nonatomic, assign) NSUInteger index;

@end

@implementation TListReaderContext

- (id) initWithArray:(NSArray *)array
{
  self = [super init];

  if (self) {
    _array = [array retain_stub];
    _index = 0;
  }

  return self;
}

- (void) dealloc
{
  [_array release_stub];
  [super dealloc_stub];
}

- (BOOL) isAtEnd
{
  return self.index >= self.array.count;
}

- (NSObject *) topObject
{
  return [self.array objectAtIndex:self.index];
}

- (void) advance
{
  ++self.index;
}

@end

typedef TListReaderContext TSetReaderContext;

@interface TWriterContext : NSObject {
  NSString* _fieldName;
}

- (id) init;
- (void) writeObject:(NSObject *) obj;
- (BOOL) isKeyContext;

@property (nonatomic, retain) NSString* fieldName;
@property (nonatomic, readonly) NSObject* object;

@end

@implementation TWriterContext

@synthesize fieldName = _fieldName;

- (id) init
{
  self = [super init];

  if (self) {
    _fieldName = nil;
  }

  return self;
}

- (void) dealloc
{
  [_fieldName release_stub];
  [super dealloc_stub];
}

- (NSObject *) object
{
  @throw [TProtocolException exceptionWithName:@"TProtocolException"
              reason:@"Attempt to call abstract base class method"];
  return nil;
}

- (void) writeObject:(NSObject *) obj
{
  @throw [TProtocolException exceptionWithName:@"TProtocolException"
              reason:@"Attempt to call abstract base class method"];
}

- (BOOL) isKeyContext
{
  return NO;
}

@end

@interface TStructWriterContext : TWriterContext {
  NSMutableDictionary* _dictionary;
}

@property (nonatomic, retain, readonly) NSMutableDictionary* dictionary;

@end

@implementation TStructWriterContext

@synthesize dictionary = _dictionary;

- (id) init
{
  self = [super init];

  if (self) {
    _dictionary = [[NSMutableDictionary alloc] init];
  }

  return self;
}

- (void) dealloc
{
  [_dictionary release_stub];
  [super dealloc_stub];
}

- (NSObject *) object
{
  return _dictionary;
}

- (void) writeObject:(NSObject *) obj
{
  [self.dictionary setObject:obj forKey:self.fieldName];
}

@end

typedef enum {
  KEY_STATE,
  VALUE_STATE
} TMapWriterState;

@interface TMapWriterContext : TWriterContext {
  NSMutableDictionary* _obj;
  NSString* _mapKey;
  TMapWriterState _state;
}

@property (nonatomic, retain, readonly) NSMutableDictionary* dictionary;
@property (nonatomic, assign) TMapWriterState state;
@property (nonatomic, retain) NSString* mapKey;

@end

@implementation TMapWriterContext

@synthesize dictionary = _dictionary;
@synthesize state = _state;
@synthesize mapKey = _mapKey;

- (id) init
{
  self = [super init];

  if (self) {
    _dictionary = [[NSMutableDictionary alloc] init];
    _state = KEY_STATE;
    _mapKey = nil;
  }

  return self;
}

- (void) dealloc
{
  [_dictionary release_stub];
  [_mapKey release_stub];
  [super dealloc_stub];
}

- (NSObject *) object
{
  return _dictionary;
}

- (void) writeObject:(NSObject *) obj
{
  switch (self.state) {
    case KEY_STATE:
      NSAssert([obj isKindOfClass:[NSString class]], @"Map key is not a string");
      self.mapKey = (NSString*) obj;
      self.state = VALUE_STATE;
      break;

    case VALUE_STATE:
      NSAssert(self.mapKey, @"Map key not set");
      [self.dictionary setObject:obj forKey:self.mapKey];
      self.mapKey = nil;
      self.state = KEY_STATE;
      break;
  }
}

- (BOOL) isKeyContext
{
  return self.state == KEY_STATE;
}

@end

@interface TListWriterContext : TWriterContext {
  NSMutableArray* _array;
}

@property (nonatomic, retain, readonly) NSMutableArray* array;

@end

@implementation TListWriterContext

@synthesize array = _array;

- (id) init
{
  self = [super init];

  if (self) {
    _array = [[NSMutableArray alloc] init];
  }

  return self;
}

- (void) dealloc
{
  [_array release_stub];
  [super dealloc_stub];
}


- (NSObject *) object
{
  return _array;
}

- (void) writeObject:(NSObject *) obj
{
  [self.array addObject:obj];
}

@end

typedef TListWriterContext TSetWriterContext;

@interface TJSONUtils : NSObject

+ (NSObject *)parseJSONData: (NSData *)data;
+ (NSObject *)parseJSONString: (NSString *)str;

+ (NSData *)serializeJSONData: (NSObject *)obj;
+ (NSString *)serializeJSONString: (NSObject *)obj;

@end

@implementation TJSONUtils

+ (NSObject *)parseJSONData:(NSData *)data
{
  NSError *error = nil;
  id obj = [NSJSONSerialization JSONObjectWithData:data options:0 error:&error];
  if (error != nil) {
    NSString *message = [NSString stringWithFormat:@"Error parsing JSON: %@", [error localizedDescription]];
    @throw [TProtocolException exceptionWithName:@"TProtocolException"
                                          reason:message];
  }

  return obj;
}

+ (NSObject *)parseJSONString:(NSString *)str
{
  return [self parseJSONData:[str dataUsingEncoding:NSUTF8StringEncoding]];
}

+ (NSData *)serializeJSONData:(NSObject *) obj
{
  NSError* error = nil;
  DLog(@"writing object: %@\n", obj);
  NSData* data = [NSJSONSerialization dataWithJSONObject:obj
                                                 options:0
                                                   error:&error];
  if (error != nil) {
    NSString *message = [NSString stringWithFormat:@"Error serializing JSON: %@", [error localizedDescription]];
    @throw [TProtocolException exceptionWithName:@"TProtocolException"
                                          reason:message];
  }

  return data;
}

+ (NSString *)serializeJSONString:(NSObject *) obj
{
  NSData *data = [TJSONUtils serializeJSONData:obj];
  return [[[NSString alloc] initWithData:data encoding:NSUTF8StringEncoding] autorelease_stub];
}

@end

@interface TObjectConverterUtils : NSObject

+ (NSNumber *) castObjectToNumber:(NSObject *)obj;
+ (NSString *) castObjectToString:(NSObject *)obj;
+ (NSArray *) castObjectToArray:(NSObject *)obj;
+ (NSDictionary *) castObjectToDictionary:(NSObject *)obj;

@end

@implementation TObjectConverterUtils

+ (NSNumber *) castObjectToNumber:(NSObject *)obj
{
  if (![obj isKindOfClass:[NSNumber class]]) {
    @throw [TProtocolException exceptionWithName:@"TProtocolException"
                                          reason:@"Value is not a number"];
  }
  return (NSNumber *) obj;
}

+ (NSString *) castObjectToString:(NSObject *)obj
{
  if (![obj isKindOfClass:[NSString class]]) {
    @throw [TProtocolException exceptionWithName:@"TProtocolException"
                                          reason:@"Value is not a string"];
  }
  return (NSString *) obj;
}

+ (NSArray *) castObjectToArray:(NSObject *)obj
{
  if (![obj isKindOfClass:[NSArray class]]) {
    @throw [TProtocolException exceptionWithName:@"TProtocolException"
                                          reason:@"Value is not an array"];
  }
  return (NSArray *) obj;
}

+ (NSDictionary *) castObjectToDictionary:(NSObject *)obj
{
  if (![obj isKindOfClass:[NSDictionary class]]) {
    @throw [TProtocolException exceptionWithName:@"TProtocolException"
                                          reason:@"Value is not a dictionary"];
  }
  return (NSDictionary *) obj;
}

@end

@protocol TObjectConverter <NSObject>

- (BOOL) toBool: (NSObject *) obj;
- (uint8_t) toByte: (NSObject *) obj;
- (short) toI16: (NSObject *) obj;
- (int32_t) toI32: (NSObject *) obj;
- (int64_t) toI64: (NSObject *) obj;
- (double) toDouble: (NSObject *) obj;
- (NSString *) toString: (NSObject *) obj;
- (NSData *) toBinary: (NSObject *) obj;
- (NSArray *) toArray: (NSObject *) obj;
- (NSDictionary *) toDictionary: (NSObject *) obj;

- (NSObject *) fromBool: (BOOL) value;
- (NSObject *) fromByte: (uint8_t) value;
- (NSObject *) fromI16: (short) value;
- (NSObject *) fromI32: (int32_t) value;
- (NSObject *) fromI64: (int64_t) value;
- (NSObject *) fromDouble: (double) value;
- (NSObject *) fromString: (NSString *) value;
- (NSObject *) fromBinary: (NSData *) data;
- (NSObject *) fromObject: (NSObject *) data;

@end

@interface TJSONKeyConverter : NSObject<TObjectConverter>

@end

@implementation TJSONKeyConverter

- (id) init
{
  return [super init];
}

- (void) dealloc
{
  [super dealloc];
}

- (NSNumber *) toNumber: (NSObject *)obj
{
  NSString *str = [TObjectConverterUtils castObjectToString:obj];
  // TODO: rethrow exception
  int64_t value = [str longLongValue];
  return [NSNumber numberWithLongLong:value];
}

- (BOOL) toBool: (NSObject *) obj
{
  NSString *str = [TObjectConverterUtils castObjectToString:obj];
  if ([str isEqualToString:@"true"]) {
    return YES;
  } else if ([str isEqualToString:@"false"]) {
    return NO;
  } else {
    @throw [TProtocolException exceptionWithName:@"TProtocolException"
                                          reason:@"Value is not a bool"];
  }
}

- (uint8_t) toByte: (NSObject *) obj
{
  return [[self toNumber:obj] charValue];
}

- (short) toI16: (NSObject *) obj
{
  return [[self toNumber:obj] shortValue];
}

- (int32_t) toI32: (NSObject *) obj
{
  return [[self toNumber:obj] intValue];
}

- (int64_t) toI64: (NSObject *) obj
{
  // TODO: rethrow exception
  return [[TObjectConverterUtils castObjectToString:obj] longLongValue];
}

- (double) toDouble: (NSObject *) obj
{
  // TODO: rethrow exception
  return [[TObjectConverterUtils castObjectToString:obj] doubleValue];
}

- (NSString *) toString: (NSObject *) obj
{
  return [TObjectConverterUtils castObjectToString:obj];
}

- (NSData *) toBinary: (NSObject *) obj
{
  NSString *base64Str = [TObjectConverterUtils castObjectToString:obj];
  NSData *data = [[[NSData alloc] initWithBase64EncodedString:base64Str options:0] autorelease_stub];
  if (!data) {
    @throw [TProtocolException exceptionWithName:@"TProtocolException"
                                          reason:@"Value is not a base 64 encoded string"];
  }
  return data;
}

- (NSObject *) toObject: (NSObject *) obj
{
  NSString *str = [TObjectConverterUtils castObjectToString:obj];
  return [TJSONUtils parseJSONString:str];
}

- (NSArray *) toArray: (NSObject *) obj
{
  return [TObjectConverterUtils castObjectToArray:[self toObject:obj]];
}

- (NSDictionary *) toDictionary: (NSObject *) obj
{
  return [TObjectConverterUtils castObjectToDictionary:[self toObject:obj]];
}

- (NSObject *) fromBool: (BOOL) value
{
  return value ? @"true" : @"false";
}

- (NSObject *) fromByte: (uint8_t) value
{
  return [NSString stringWithFormat:@"%hhd", (int8_t) value];
}

- (NSObject *) fromI16: (short) value
{
  return [NSString stringWithFormat:@"%hd", value];
}

- (NSObject *) fromI32: (int32_t) value
{
  return [NSString stringWithFormat:@"%d", value];
}

- (NSObject *) fromI64: (int64_t) value
{
  return [NSString stringWithFormat:@"%lld", value];
}

- (NSObject *) fromDouble: (double) value
{
  // TODO(ahilss): print in decimal?
  return [NSString stringWithFormat:@"%a", value];
}

- (NSObject *) fromString: (NSString *) str
{
  return str;
}

- (NSObject *) fromBinary: (NSData *) data
{
  return [data base64EncodedStringWithOptions:0];
}

- (NSObject *) fromObject: (NSObject *) obj
{
  return [TJSONUtils serializeJSONString:obj];
}

@end

@interface TJSONValueConverter : NSObject<TObjectConverter>

@end

@implementation TJSONValueConverter

- (id) init
{
  return [super init];
}

- (void) dealloc
{
  [super dealloc];
}

- (NSNumber *) toNumber: (NSObject *)obj
{
  return [TObjectConverterUtils castObjectToNumber:obj];
}

- (BOOL) toBool: (NSObject *) obj
{
  return [[self toNumber:obj] boolValue];
}

- (uint8_t) toByte: (NSObject *) obj
{
  return [[self toNumber:obj] charValue];
}

- (short) toI16: (NSObject *) obj
{
  return [[self toNumber:obj] shortValue];
}

- (int32_t) toI32: (NSObject *) obj
{
  return [[self toNumber:obj] intValue];
}

- (int64_t) toI64: (NSObject *) obj
{
  return [[self toNumber:obj] longLongValue];
}

- (double) toDouble: (NSObject *) obj
{
  return [[self toNumber:obj] doubleValue];
}

- (NSString *) toString: (NSObject *) obj
{
  return [TObjectConverterUtils castObjectToString:obj];
}

- (NSData *) toBinary: (NSObject *) obj
{
  NSString *base64Str = [TObjectConverterUtils castObjectToString:obj];
  NSData *data = [[[NSData alloc] initWithBase64EncodedString:base64Str options:0] autorelease_stub];
  if (!data) {
    @throw [TProtocolException exceptionWithName:@"TProtocolException"
                                          reason:@"Value is not a base 64 encoded string"];
  }
  return data;
}

- (NSObject *) toObject: (NSObject *) obj
{
  return obj;
}

- (NSArray *) toArray: (NSObject *) obj
{
  return [TObjectConverterUtils castObjectToArray:[self toObject:obj]];
}

- (NSDictionary *) toDictionary: (NSObject *) obj
{
  return [TObjectConverterUtils castObjectToDictionary:[self toObject:obj]];
}

- (NSObject *) fromBool: (BOOL) value
{
  return [NSNumber numberWithBool:value];
}

- (NSObject *) fromByte: (uint8_t) value
{
  return [NSNumber numberWithUnsignedChar:value];
}

- (NSObject *) fromI16: (short) value
{
  return [NSNumber numberWithShort:value];
}

- (NSObject *) fromI32: (int32_t) value
{
  return [NSNumber numberWithInt:value];
}

- (NSObject *) fromI64: (int64_t) value
{
  return [NSNumber numberWithLongLong:value];
}

- (NSObject *) fromDouble: (double) value
{
  return [NSNumber numberWithDouble:value];
}

- (NSObject *) fromString: (NSString *) str
{
  return str;
}

- (NSObject *) fromBinary: (NSData *) data
{
  return [data base64EncodedStringWithOptions:0];
}

- (NSObject *) fromObject: (NSObject *) obj
{
  return obj;
}

@end

@interface TSimpleJSONProtocol() {

@private
  id <TTransport> _transport;
  NSDictionary* _rootObj;
  NSMutableArray* _readerStack;
  NSMutableArray* _writerStack;
  TJSONKeyConverter* _keyConverter;
  TJSONValueConverter* _valueConverter;
}

- (void) pushWriterContext:(TWriterContext*)context;
- (void) popWriterContext;

@property (nonatomic, retain) NSDictionary* rootObj;
@property (nonatomic, retain) NSMutableArray* readerStack;
@property (nonatomic, retain) NSMutableArray* writerStack;
@property (nonatomic, readonly, retain) TWriterContext* topWriterContext;
@property (nonatomic, readonly) id <TObjectConverter> converter;

@end;

@implementation TSimpleJSONProtocol

@synthesize rootObj = _rootObj;
@synthesize readerStack = _readerStack;
@synthesize writerStack = _writerStack;
@synthesize converter = _converter;

- (id) initWithTransport: (id <TTransport>) transport
{
  self = [super init];

  if (self) {
    _transport = [transport retain_stub];
    _rootObj = nil;
    _readerStack = [[NSMutableArray alloc] init];
    _writerStack = [[NSMutableArray alloc] init];
    _keyConverter = [[TJSONKeyConverter alloc] init];
    _valueConverter = [[TJSONValueConverter alloc] init];
  }

  return self;
}

- (void) dealloc
{
  [_valueConverter release_stub];
  [_keyConverter release_stub];
  [_writerStack release_stub];
  [_readerStack release_stub];
  [_rootObj release_stub];
  [_transport release_stub];
  [super dealloc_stub];
}

- (id <TTransport>) transport
{
  return _transport;
}

- (void) writeObject:(id)obj
{
  DLog(@"writeObject: %@", obj);

  if (self.writerStack.count == 0) {
    self.rootObj = obj;
  } else {
    [self.topWriterContext writeObject:obj];
  }
}

- (void) pushWriterContext:(TWriterContext *)context
{
  [self.writerStack addObject:context];
  DLog(@"writerStack size: %lu", (unsigned long) self.writerStack.count);
}

- (void) popWriterContext
{
  [self.writerStack removeLastObject];
}

- (TWriterContext*) topWriterContext
{
  return [self.writerStack lastObject];
}

- (void) popWriterContextAndWriteObject
{
  NSObject *obj = [[[self topWriterContext] object] retain_stub];
  [self popWriterContext];
  [self writeObject:[self.writerConverter fromObject:obj]];
  [obj release_stub];
}

- (id <TObjectConverter>) writerConverter
{
  return [self.topWriterContext isKeyContext] ? _keyConverter : _valueConverter;
}

- (void) pushReaderContext:(TReaderContext *)context
{
  [self.readerStack addObject:context];
  DLog(@"pushReaderContext: %lu", (unsigned long) self.readerStack.count);
}

- (void) popReaderContext
{
  DLog(@"popReaderContext: %lu", (unsigned long) self.readerStack.count);
  [self.readerStack removeLastObject];
}

- (TReaderContext*) topReaderContext
{
  NSAssert(self.readerStack.count > 0, @"Attempt to read top of empty stack");
  return [self.readerStack lastObject];
}

- (id <TObjectConverter>) readerConverter
{
  return [self.topReaderContext isKeyContext] ? _keyConverter : _valueConverter;
}

- (NSData *) readAllDataFromTransport
{
  const int kReadBufferSize = 1024;
  const int kMaxReadSize = 1 << 30;
  uint8_t readBuffer[kReadBufferSize];

  NSMutableData *data = [[[NSMutableData alloc] init] autorelease_stub];

  BOOL hasMoreData = YES;

  while (hasMoreData && [data length] < kMaxReadSize) {
    int numRead = [self.transport read:readBuffer offset:0 length:kReadBufferSize];
    [data appendBytes:readBuffer length:numRead];

    if (numRead < kReadBufferSize) {
      hasMoreData = NO;
    }
  }

  return data;
}

- (void) writeDataToTransport: (NSData *)data
{
  [self.transport write:data.bytes offset:0 length:(int) data.length];
}

- (NSDictionary *) parseDictionaryFromJSONData:(NSData *) data
{
  return [TObjectConverterUtils castObjectToDictionary:[TJSONUtils parseJSONData:data]];
}

- (id)readObject
{
  return [self.topReaderContext nextObject];
}

- (void) readMessageBeginReturningName: (NSString **) name
                                  type: (int *) type
                            sequenceID: (int *) sequenceID
{
  @throw [TProtocolException exceptionWithName:@"TProtocolException"
                                        reason:@"Not implemented"];
}

- (void) readMessageEnd
{
  @throw [TProtocolException exceptionWithName:@"TProtocolException"
                                        reason:@"Not implemented"];
}

- (void) readStructBeginReturningName: (NSString **) name
{
  DLog(@"readStructBegin");
  NSDictionary *dictionary = nil;

  if (self.readerStack.count == 0) {
    dictionary = [self parseDictionaryFromJSONData:[self readAllDataFromTransport]];
  } else {
    dictionary = [self.readerConverter toDictionary:[self readObject]];
  }

  TReaderContext *readerContext = [[TStructReaderContext alloc] initWithDictionary:dictionary];
  [self pushReaderContext:readerContext];
  [readerContext release_stub];
}

- (void) readStructEnd
{
  DLog(@"readStructEnd");
  [self popReaderContext];
}

- (void) readFieldBeginReturningName: (NSString **) name
                                type: (int *) fieldType
                             fieldID: (int *) fieldID
{
  NSAssert([self.topReaderContext isKindOfClass:[TStructReaderContext class]],
           @"Attempt to read field outside of struct context");

  TStructReaderContext *readerContext = (TStructReaderContext *) self.topReaderContext;

  *fieldID = kFieldIDUnknown;

  if (![readerContext isAtEnd]) {
    if (![[readerContext nextValue] isKindOfClass:[NSNull class]]) {
      *name = [readerContext nextKey];
    } else {
      *name = @"";
    }
    *fieldType = TType_UNKNOWN;
  } else {
    *name = @"";
    *fieldType = TType_STOP;
  }

  DLog(@"readFieldBegin: %@", *name);
}

- (void) readFieldEnd
{
  DLog(@"readFieldEnd");
}

- (void) readMapBeginReturningKeyType: (int *) keyType
                            valueType: (int *) valueType
                                 size: (int *) size
{
  DLog(@"readMapBegin");

  NSDictionary *dictionary = [self.readerConverter toDictionary:[self readObject]];
  *size = (int) dictionary.count;

  TReaderContext *readerContext = [[TMapReaderContext alloc] initWithDictionary:dictionary];
  [self pushReaderContext:readerContext];
  [readerContext release_stub];
}

- (void) readMapEnd
{
  DLog(@"readMapEnd");
  [self popReaderContext];
}

- (void) readSetBeginReturningElementType: (int *) elementType
                                     size: (int *) size
{
  DLog(@"readSetBegin");
  NSArray *array = [self.readerConverter toArray:[self readObject]];
  *size = (int) array.count;
  [self pushReaderContext:[[TSetReaderContext alloc] initWithArray:array]];

  TReaderContext *readerContext = [[TSetReaderContext alloc] initWithArray:array];
  [self pushReaderContext:readerContext];
  [readerContext release_stub];
}

- (void) readSetEnd
{
  DLog(@"readSetEnd");
  [self popReaderContext];
}

- (void) readListBeginReturningElementType: (int *) elementType
                                      size: (int *) size
{
  DLog(@"readListBegin");
  NSArray *array = [self.readerConverter toArray:[self readObject]];
  *size = (int) array.count;

  TReaderContext *readerContext = [[TListReaderContext alloc] initWithArray:array];
  [self pushReaderContext:readerContext];
  [readerContext release_stub];
}

- (void) readListEnd
{
  DLog(@"readListEnd");
  [self popReaderContext];
}

- (BOOL) readBool
{
  DLog(@"readBool");
  return [self.readerConverter toBool:[self readObject]];
}

- (uint8_t) readByte
{
  DLog(@"readByte");
  return [self.readerConverter toByte:[self readObject]];
}

- (short) readI16
{
  DLog(@"readShort");
  return [self.readerConverter toI16:[self readObject]];
}

- (int32_t) readI32
{
  DLog(@"readI32");
  return [self.readerConverter toI32:[self readObject]];
}

- (int64_t) readI64;
{
  DLog(@"readI64");
  return [self.readerConverter toI64:[self readObject]];
}

- (double) readDouble;
{
  return [self.readerConverter toDouble:[self readObject]];
}

- (NSString *) readString
{
  DLog(@"readString");
  return [self.readerConverter toString:[self readObject]];
}

- (NSData *) readBinary
{
  DLog(@"readBinary");
  return [self.readerConverter toBinary:[self readObject]];
}

- (void) readUnknown
{
    [self readObject];
}

- (void) writeMessageBeginWithName: (NSString *) name
                              type: (int) messageType
                        sequenceID: (int) sequenceID
{
  @throw [TProtocolException exceptionWithName:@"TProtocolException"
                                        reason:@"Not implemented"];
}

- (void) writeMessageEnd
{
  @throw [TProtocolException exceptionWithName:@"TProtocolException"
                                        reason:@"Not implemented"];
}

- (void) writeStructBeginWithName: (NSString *) name
{
  DLog(@"writeStructBegin: %@", name);
  TWriterContext *writerContext = [[TStructWriterContext alloc] init];
  [self pushWriterContext:writerContext];
  [writerContext release_stub];
}

- (void) writeStructEnd
{
  DLog(@"writeStructEnd");
  [self popWriterContextAndWriteObject];

  if (self.writerStack.count == 0) {
    NSAssert(self.rootObj, @"Root object has not been set");
    [self writeDataToTransport:[TJSONUtils serializeJSONData:self.rootObj]];
  }
}

- (void) writeFieldBeginWithName: (NSString *) name
                            type: (int) fieldType
                         fieldID: (int) fieldID
{
  DLog(@"writeFieldBegin: %@", name);
  NSAssert([self.topWriterContext isKindOfClass:[TStructWriterContext class]],
           @"Attempt to write field outside of struct context");
  TStructWriterContext *writerContext = (TStructWriterContext *) self.topWriterContext;
  writerContext.fieldName = name;
}

- (void) writeFieldEnd
{
  DLog(@"writeFieldEnd");
}

- (void) writeFieldStop
{
}

- (void) writeMapBeginWithKeyType: (int) keyType
                        valueType: (int) valueType
                             size: (int) size
{
  TWriterContext *writerContext = [[TMapWriterContext alloc] init];
  [self pushWriterContext:writerContext];
  [writerContext release_stub];
}

- (void) writeMapEnd
{
  [self popWriterContextAndWriteObject];
}

- (void) writeListBeginWithElementType: (int) elementType
                                  size: (int) size
{
  TWriterContext *writerContext = [[TListWriterContext alloc] init];
  [self pushWriterContext:writerContext];
  [writerContext release_stub];
}

- (void) writeListEnd
{
  [self popWriterContextAndWriteObject];
}

- (void) writeSetBeginWithElementType: (int) elementType
                                 size: (int) size
{
  TWriterContext *writerContext = [[TSetWriterContext alloc] init];
  [self pushWriterContext:writerContext];
  [writerContext release_stub];
}

- (void) writeSetEnd
{
  [self popWriterContextAndWriteObject];
}

- (void) writeBool: (BOOL) value
{
  [self writeObject:[self.writerConverter fromBool:value]];
}

- (void) writeByte: (uint8_t) value
{
  [self writeObject:[self.writerConverter fromByte:value]];
}

- (void) writeI16: (short) value
{
  [self writeObject:[self.writerConverter fromI16:value]];
}

- (void) writeI32: (int32_t) value
{
  [self writeObject:[self.writerConverter fromI32:value]];
}

- (void) writeI64: (int64_t) value
{
  [self writeObject:[self.writerConverter fromI64:value]];
}

- (void) writeDouble: (double) value
{
  [self writeObject:[self.writerConverter fromDouble:value]];
}

- (void) writeString: (NSString *) value
{
  [self writeObject:[self.writerConverter fromString:value]];
}

- (void) writeBinary: (NSData *) value
{
  [self writeObject:[self.writerConverter fromBinary:value]];
}

@end

@implementation TSimpleJSONProtocolFactory

+ (TSimpleJSONProtocolFactory *) sharedFactory {
  static TSimpleJSONProtocolFactory * sharedFactory = nil;

  @synchronized(self) {
    if (sharedFactory == nil) {
      sharedFactory = [[TSimpleJSONProtocolFactory alloc] init];
    }
  }

  return sharedFactory;
}

- (TSimpleJSONProtocol *) newProtocolOnTransport: (id <TTransport>) transport {
  return [[TSimpleJSONProtocol alloc] initWithTransport: transport];
}

@end

