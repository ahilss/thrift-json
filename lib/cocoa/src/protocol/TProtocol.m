#import "TProtocol.h"

const int kFieldIDUnknown = -1;

@implementation TFieldDescriptor

@synthesize type = _type;
@synthesize fieldID = _fieldID;

- (id)initWithType:(TType)type fieldID:(int)fieldID {
  self = [super init];

  if (self) {
    _type = type;
    _fieldID = fieldID;
  }

  return self;
}


@end
