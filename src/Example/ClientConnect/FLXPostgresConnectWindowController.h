
#import <Cocoa/Cocoa.h>
#import <PostgresClientKit/PostgresClientKit.h>

@interface FLXPostgresConnectWindowController : NSWindowController {
	NSNetServiceBrowser* netServiceBrowser;
	NSInteger returnCode;
	FLXPostgresConnection* connection;	
	IBOutlet NSArrayController* ibSettingsController;
}

@property (retain) NSNetServiceBrowser* netServiceBrowser;
@property (retain) FLXPostgresConnection* connection;
@property (assign) NSInteger returnCode;

// methods
-(void)beginSheetForWindow:(NSWindow* )mainWindow modalDelegate:(id)theDelegate didEndSelector:(SEL)theSelector;
-(BOOL)isVisible;

// IBAction
-(IBAction)doEndSheet:(id)sender;
-(IBAction)doAdvancedSettings:(id)sender;

@end
