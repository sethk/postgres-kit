#import "PostgresClientKit.h"
#import "PostgresClientKitPrivate.h"
#import "FLXPostgresArray.h"
#import "FLXPostgresTypeNSArray.h"
#import "FLXPostgresTypeNSNumber.h"

FLXPostgresOid FLXPostgresTypeNSArrayTypes[] = {
	FLXPostgresOidArrayBool,FLXPostgresOidArrayData,FLXPostgresOidArrayChar,FLXPostgresOidArrayName,FLXPostgresOidArrayInt2,
	FLXPostgresOidArrayInt4,FLXPostgresOidArrayText,FLXPostgresOidArrayVarchar,FLXPostgresOidArrayInt8,FLXPostgresOidArrayFloat4,
	FLXPostgresOidArrayFloat8,FLXPostgresOidArrayMacAddr,FLXPostgresOidArrayIPAddr,
	0
};

// maximum number of dimensions for arrays
#define ARRAY_MAXDIM   6

////////////////////////////////////////////////////////////////////////////////

@implementation FLXPostgresTypeNSArray

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
	return FLXPostgresTypeNSArrayTypes;
}

-(Class)nativeClass {
	return [NSArray class];
}

////////////////////////////////////////////////////////////////////////////////////////////////
// returns YES if the NSArray to bind includes objects of the same class (and objects can be
// bound). Also returns if there are any NULL objects, and the Oid of the objects.

-(BOOL)_validBoundValueForArray:(NSArray* )theArray hasNull:(BOOL* )hasNull type:(FLXPostgresOid* )theType {
	NSParameterAssert(theArray);
	NSParameterAssert(hasNull);
	NSParameterAssert(theType);
	
	(*theType) = 0;     // init type
	(*hasNull) = NO; 	// init null flag
	
	// iterate through the objects
	for(NSObject* theObject in theArray) {
		// we allow NSNull objects regardless
		if([theObject isKindOfClass:[NSNull class]]) {
			(*hasNull) = YES;
			continue;
		}
		// we don't allow nested arrays
		if([theObject isKindOfClass:[NSArray class]]) {
			return NO;
		}
		// set the type
		id<FLXPostgresTypeProtocol> theTypeHandler = [m_theConnection _typeHandlerForClass:[theObject class]];
		if(theTypeHandler==NULL) {
			return NO;
		}
		FLXPostgresOid theType2 = [theTypeHandler remoteTypes][0];
		if((*theType)==0) {
			(*theType) = theType2;
		} else if((*theType) != theType2) {
			return NO;
		}
	}	
	
	// if type is zero here, then set the type to text
	if((*theType)==0) {
		(*theType) = FLXPostgresOidText;
	}	
	
	return YES;
}

////////////////////////////////////////////////////////////////////////////////////////////////
// return number of dimensions for NSArray - currently zero or one

-(SInt32)_dimensionsForArray:(NSArray* )theArray {
	NSParameterAssert(theArray);
	if([theArray count]==0) return 0;
	return 1;
}

////////////////////////////////////////////////////////////////////////////////////////////////
// return type based on tuple type

-(FLXPostgresOid)_arrayTypeForElementType:(FLXPostgresOid)theType {
	switch(theType) {
		case FLXPostgresOidBool:
			return FLXPostgresOidArrayBool;
		case FLXPostgresOidData:
			return FLXPostgresOidArrayData;
		case FLXPostgresOidChar:
			return FLXPostgresOidArrayChar;
		case FLXPostgresOidName:
			return FLXPostgresOidArrayName;
		case FLXPostgresOidInt2:
			return FLXPostgresOidArrayInt2;
		case FLXPostgresOidInt4:
			return FLXPostgresOidArrayInt4;
		case FLXPostgresOidText:
			return FLXPostgresOidArrayText;
		case FLXPostgresOidVarchar:
			return FLXPostgresOidArrayVarchar;
		case FLXPostgresOidInt8:
			return FLXPostgresOidArrayInt8;
		case FLXPostgresOidFloat4:
			return FLXPostgresOidArrayFloat4;
		case FLXPostgresOidFloat8:
			return FLXPostgresOidArrayFloat8;
		case FLXPostgresOidMacAddr:
			return FLXPostgresOidArrayMacAddr;
		case FLXPostgresOidIPAddr:
			return FLXPostgresOidArrayIPAddr;
	}
	return 0;
}

////////////////////////////////////////////////////////////////////////////////////////////////
// return bound NSData object

-(NSData* )remoteDataFromObject:(id)theObject type:(FLXPostgresOid* )theType {
	NSParameterAssert(theObject);
	NSParameterAssert([theObject isKindOfClass:[NSArray class]]);
	NSParameterAssert(theType);
	
	BOOL hasNull;
	FLXPostgresOid theElementType;
	NSMutableData* theBytes = [NSMutableData data];
	
	// arrays must be empty or one-dimensional, with either one supported object class type or NSNull
	if([self _validBoundValueForArray:(NSArray* )theObject hasNull:&hasNull type:&theElementType]==NO) {
		[m_theConnection _noticeProcessorWithMessage:@"Unsupported array tuples cannot be bound"];
		return nil;
	}
	NSParameterAssert(theElementType);
	
	// obtain array type - 0 means unsupported
	FLXPostgresOid theArrayType = [self _arrayTypeForElementType:theElementType];
	if(theArrayType==0) {
		[m_theConnection _noticeProcessorWithMessage:@"Unsupported array type cannot be bound"];
		return nil;
	}
	// set the type
	(*theType) = theArrayType;
	
	// insert number of dimensions
	FLXPostgresTypeNSNumber *numberHandler = (FLXPostgresTypeNSNumber* )[m_theConnection _typeHandlerForClass:[NSNumber class]];
	SInt32 dim = [self _dimensionsForArray:(NSArray* )theObject];
	NSParameterAssert(dim >= 0 && dim <= ARRAY_MAXDIM);
	[theBytes appendData:[numberHandler remoteDataFromInt32:dim]];
	
	// set flags - should be 0 or 1
	[theBytes appendData:[numberHandler remoteDataFromInt32:(hasNull ? 1 : 0)]];
	
	// set the type of the tuples in the array
	[theBytes appendData:[numberHandler remoteDataFromInt32:theElementType]];
	
	// return if dimensions is zero
	if(dim==0) {
		return theBytes;
	}
	
	// for each dimension, output the number of tuples in the dimension
	// and the lower bound (which is always zero)
	NSParameterAssert(dim==0 || dim==1);
	SInt32 theCount = [(NSArray* )theObject count];
	SInt32 theLowerBound = 1;
	NSParameterAssert([(NSArray* )theObject count]==theCount);
	[theBytes appendData:[numberHandler remoteDataFromInt32:theCount]];
	[theBytes appendData:[numberHandler remoteDataFromInt32:theLowerBound]];
	
	id<FLXPostgresTypeProtocol> theElementTypeHandler = [m_theConnection _typeHandlerForRemoteType:theElementType];
	if(!theElementTypeHandler) {
		[m_theConnection _noticeProcessorWithMessage:[NSString stringWithFormat:@"Unsupported array element type %d",theElementType]];
	}

	// append the objects
	NSUInteger i = 0;
	for(NSObject* theElement in (NSArray* )theObject) {
		i++;
		if([theElement isKindOfClass:[NSNull class]]) {
			// output 0xFFFFFFFF
			[theBytes appendData:[numberHandler remoteDataFromInt32:((SInt32)-1)]];
			continue;
		}
		FLXPostgresOid theType;
		NSData* theBoundElement = [theElementTypeHandler remoteDataFromObject:theElement type:&theType];
		if(theBoundElement==nil) {
			[m_theConnection _noticeProcessorWithMessage:[NSString stringWithFormat:@"Unable to bind array object (tuple %d)",i]];
			return nil;
		}			
		if([theBoundElement isKindOfClass:[NSData class]]==NO) {
			[m_theConnection _noticeProcessorWithMessage:[NSString stringWithFormat:@"Unable to bind non-data array object (tuple %d)",i]];
			return nil;			
		}
		if(theType != theElementType) {
			[m_theConnection _noticeProcessorWithMessage:[NSString stringWithFormat:@"Unable to bind array object (tuple %d), unexpected type",i]];
			return nil;			
		}
		if([theBoundElement length] > ((NSUInteger)0x7FFFFFFF)) {
			[m_theConnection _noticeProcessorWithMessage:[NSString stringWithFormat:@"Unable to bind array object (tuple %d), beyond capacity",i]];
			return nil;			
		}			
		// TODO: ensure length of data is no greater than 0x7FFFFFFF
		[theBytes appendData:[numberHandler remoteDataFromInt32:[theBoundElement length]]];
		[theBytes appendData:theBoundElement];			   
	}
	
	return theBytes;	
}

////////////////////////////////////////////////////////////////////////////////////////////////
// arrays

-(id)objectFromRemoteData:(const void* )theBytes length:(NSUInteger)theLength type:(FLXPostgresOid)theType {
	NSParameterAssert(theBytes);
	
	FLXPostgresTypeNSNumber *numberHandler = (FLXPostgresTypeNSNumber* )[m_theConnection _typeHandlerForClass:[NSNumber class]];
	// use 4 byte alignment
	const UInt32* thePtr = theBytes;
	// get number of dimensions - we allow zero-dimension arrays
	NSInteger dim = [[numberHandler integerObjectFromBytes:(thePtr++) length:4] integerValue];
	NSParameterAssert(dim >= 0 && dim <= ARRAY_MAXDIM);
	NSParameterAssert(thePtr <= (const UInt32* )((const UInt8* )theBytes + theLength));
	// return empty array if dim is zero
	if(dim==0) return [NSArray array];	
	// get flags - should be zero or one
	NSInteger flags = [[numberHandler integerObjectFromBytes:(thePtr++) length:4] integerValue];
	NSParameterAssert(flags==0 || flags==1);
	NSParameterAssert(thePtr <= (const UInt32* )((const UInt8* )theBytes + theLength));
	// get type of array
	FLXPostgresOid type = [[numberHandler unsignedIntegerObjectFromBytes:(thePtr++) length:4] unsignedIntegerValue];
	NSParameterAssert(type==theType);
	NSParameterAssert(thePtr <= (const UInt32* )((const UInt8* )theBytes + theLength));
	
	// create an array to hold tuples
	FLXPostgresArray* theArray = [FLXPostgresArray arrayWithDimensions:dim type:type];
	NSParameterAssert(theArray);
	
	// for each dimension, retrieve dimension and lower bound
	NSInteger tuples = dim ?  1 : 0;
	for(NSInteger i = 0; i < dim; i++) {
		NSInteger dimsize = [[numberHandler integerObjectFromBytes:(thePtr++) length:4] integerValue];
		NSParameterAssert(thePtr <= (const UInt32* )((const UInt8* )theBytes + theLength));
		NSInteger bound =  [[numberHandler integerObjectFromBytes:(thePtr++) length:4] integerValue];
		NSParameterAssert(thePtr <= (const UInt32* )((const UInt8* )theBytes + theLength));
		NSParameterAssert(dimsize > 0);
		NSParameterAssert(bound >= 0);		
		// set dim-n size and lower bound
		[theArray setDimension:i size:dimsize lowerBound:bound];
		// calculate number of tuples
		tuples = tuples * dimsize;
	}	
	// iterate through the tuples
	for(NSInteger i = 0; i < tuples; i++) {
		NSUInteger length = [[numberHandler unsignedIntegerObjectFromBytes:(thePtr++) length:4] unsignedIntegerValue];
		NSParameterAssert(thePtr <= (const UInt32* )((const UInt8* )theBytes + theLength));
		NSObject* theObject = nil;
		if(length==((NSUInteger)0xFFFFFFFF)) {
			theObject = [NSNull null];
			length = 0;
		} else {
			theObject = [numberHandler objectFromRemoteData:thePtr length:length type:theType];
		}
		NSParameterAssert(theObject);
		// add tuple
		[theArray addTuple:theObject];
		// increment ptr by bytes
		thePtr = (const UInt32* )((const UInt8* )thePtr + length);
		NSParameterAssert(thePtr <= (const UInt32* )((const UInt8* )theBytes + theLength));
	}
	
	// if the array is one-dimensional, return an NSArray or else return the FLXPostgresArray type
	if(dim==1) {
		return [theArray array];
	} else {
		return theArray;
	}
}

-(NSString* )quotedStringFromObject:(id)theObject {
	NSMutableString* quotedString = [NSMutableString stringWithString:@"'{"];
	[quotedString appendString:@"}'"];
	return quotedString;
}

@end
