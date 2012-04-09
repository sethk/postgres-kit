
@interface FLXPostgresServer (Private)
-(BOOL)_createPath:(NSString* )thePath error:(NSError **)pError;
-(NSString* )_backupFilePathForFolder:(NSString* )thePath;
-(int)_processIdentifierFromDataPath;
-(void)_delegateServerMessage:(NSString* )theMessage;
-(void)_delegateServerMessageFromData:(NSData* )theData;
-(void)_delegateServerStateDidChange:(NSString* )theMessage;  
-(void)_delegateBackupStateDidChange:(NSString* )theMessage;
-(NSString* )_messageFromState:(FLXServerState)theState;
-(int)_doesProcessExist:(int)thePid;
@end
