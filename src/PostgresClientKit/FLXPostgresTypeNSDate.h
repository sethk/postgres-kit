
@interface FLXPostgresTypeNSDate : NSObject <FLXPostgresTypeProtocol> {
	FLXPostgresConnection* m_theConnection;
	NSDateFormatter *m_theTimestampFormatter;
}

@end
