
@interface FLXPostgresConnection (Utils)

-(NSArray* )databases;
-(BOOL)createDatabase:(NSString* )theName;
-(BOOL)databaseExistsWithName:(NSString* )theName;
-(NSArray* )tables;
-(NSArray* )schemas;
-(NSArray* )tablesForSchema:(NSString* )theName;
-(NSArray* )tablesForSchemas:(NSArray* )theNames;
-(BOOL)tableExistsWithName:(NSString* )theTable inSchema:(NSString* )theSchema;
-(NSString* )quoteArray:(NSArray* )theArray;

-(NSString* )primaryKeyForTable:(NSString* )theTable inSchema:(NSString* )theSchema;
-(NSArray* )columnNamesForTable:(NSString* )theTable inSchema:(NSString* )theSchema;
@end
