//
//  PostgresPrefPaneBindings.h
//  postgresql
//
//  Created by David Thorpe on 27/02/2009.
//  Copyright 2009 __MyCompanyName__. All rights reserved.
//

#import <Cocoa/Cocoa.h>

@interface PostgresPrefPaneBindings : NSObject {
	NSString* bindServerVersion;
	NSString* bindServerStatus;
	NSString* bindDiskUsage;	
	NSString* bindProcessorUsage;	
	NSImage* bindServerStatusImage;	
	NSUInteger bindServerPort;
	NSUInteger bindServerPortMinValue;
	NSUInteger bindServerPortMaxValue;
	BOOL bindServerPortEnabled;	
	BOOL bindIsRemoteAccess;
	BOOL bindIsRemoteAccessEnabled;
	NSInteger bindPortMatrixIndex;
	BOOL bindPortMatrixEnabled;		
	NSInteger bindTabViewIndex;	
	BOOL bindIsBackupEnabled;	
	NSInteger bindBackupIntervalTag;
	BOOL bindBackupIntervalEnabled;	
	NSInteger bindBackupFreeSpaceTag;
	BOOL bindBackupFreeSpaceEnabled;	
}

@property (retain) NSString* bindServerVersion;
@property (retain) NSString* bindServerStatus;
@property (retain) NSString* bindDiskUsage;
@property (retain) NSString* bindProcessorUsage;
@property (retain) NSImage* bindServerStatusImage;

@property (assign) NSUInteger bindServerPort;
@property (assign) NSUInteger bindServerPortMinValue;
@property (assign) NSUInteger bindServerPortMaxValue;
@property (assign) BOOL bindServerPortEnabled;

@property (assign) BOOL bindIsRemoteAccess;
@property (assign) BOOL bindIsRemoteAccessEnabled;

@property (assign) NSInteger bindPortMatrixIndex;
@property (assign) BOOL bindPortMatrixEnabled;

@property (assign) NSInteger bindTabViewIndex;

@property (assign) BOOL bindIsBackupEnabled;
@property (assign) NSInteger bindBackupIntervalTag;
@property (assign) BOOL bindBackupIntervalEnabled;
@property (assign) NSInteger bindBackupFreeSpaceTag;
@property (assign) BOOL bindBackupFreeSpaceEnabled;

// methods
-(void)setBackupIntervalTagFromInterval:(NSTimeInterval)theInterval;
-(NSTimeInterval)backupTimeIntervalFromTag;

-(void)setBackupFreeSpaceTagFromPercent:(NSInteger)thePercent;
-(NSInteger)backupFreeSpacePercentFromTag;

@end
