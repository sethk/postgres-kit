#import "PostgresClientKit.h"
#import "PostgresClientKitPrivate.h"
#import "FLXPostgresTypeNSData.h"

////////////////////////////////////////////////////////////////////////////////

FLXPostgresOid FLXPostgresTypeNSDataTypes[] = {
	FLXPostgresOidData,0
};

////////////////////////////////////////////////////////////////////////////////

@implementation FLXPostgresTypeNSData

-(id)initWithConnection:(FLXPostgresConnection* )theConnection {
	NSParameterAssert(theConnection);
	self = [super init];
	if(self != nil) {
		m_theConnection = [theConnection retain];
	}
	return self;
}

-(void)dealloc {
	[m_theConnection release];
	[super dealloc];
}

////////////////////////////////////////////////////////////////////////////////

-(FLXPostgresOid* )remoteTypes {
	return FLXPostgresTypeNSDataTypes;
}

-(Class)nativeClass {
	return [NSData class];
}

////////////////////////////////////////////////////////////////////////////////

-(NSData* )remoteDataFromObject:(id)theObject type:(FLXPostgresOid* )theType {
	NSParameterAssert(theObject);
	NSParameterAssert([theObject isKindOfClass:[NSData class]]);
	NSParameterAssert(theType);
	(*theType) = FLXPostgresOidData;		
	return (NSData* )theObject;
}

-(id)objectFromRemoteData:(const void* )theBytes length:(NSUInteger)theLength type:(FLXPostgresOid)theType {
	NSParameterAssert(theBytes);
	return [NSData dataWithBytes:theBytes length:theLength];	
}

-(NSString* )quotedStringFromObject:(id)theObject {
	NSParameterAssert(theObject);
	NSParameterAssert([theObject isKindOfClass:[NSData class]]);
	size_t theLength = 0;
	unsigned char* theBuffer = PQescapeByteaConn([m_theConnection PGconn],[(NSData* )theObject bytes],[(NSData* )theObject length],&theLength);
	if(theBuffer==nil) {
		return nil;
	}
	NSMutableString* theNewString = [[NSMutableString alloc] initWithBytesNoCopy:theBuffer length:(theLength-1) encoding:NSUTF8StringEncoding freeWhenDone:YES];
	// add quotes
	[theNewString appendString:@"'"];
	[theNewString insertString:@"'" atIndex:0];
	// return the string
	return [theNewString autorelease];  
}

@end
