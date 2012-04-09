
@interface FLXPostgresTypeNSNumber : NSObject <FLXPostgresTypeProtocol> {
	FLXPostgresConnection* m_theConnection;
}

-(SInt16)int16FromBytes:(const void* )theBytes;
-(SInt32)int32FromBytes:(const void* )theBytes;
-(SInt64)int64FromBytes:(const void* )theBytes;
-(UInt16)unsignedInt16FromBytes:(const void* )theBytes;
-(UInt32)unsignedInt32FromBytes:(const void* )theBytes;
-(UInt64)unsignedInt64FromBytes:(const void* )theBytes;
-(NSNumber* )integerObjectFromBytes:(const void* )theBytes length:(NSUInteger)theLength;
-(NSNumber* )unsignedIntegerObjectFromBytes:(const void* )theBytes length:(NSUInteger)theLength;
-(NSData* )remoteDataFromInt64:(SInt64)theValue;
-(NSData* )remoteDataFromInt32:(SInt32)theValue;
-(NSData* )remoteDataFromInt16:(SInt16)theValue;
-(Float32)float32FromBytes:(const void* )theBytes;
-(Float64)float64FromBytes:(const void* )theBytes;
-(NSData* )remoteDataFromFloat32:(Float32)theValue;
-(NSData* )remoteDataFromFloat64:(Float64)theValue;
-(BOOL)booleanFromBytes:(const void* )theBytes;
-(NSNumber* )booleanObjectFromBytes:(const void* )theBytes length:(NSUInteger)theLength;
-(NSData* )remoteDataFromBoolean:(BOOL)theValue;

@end

