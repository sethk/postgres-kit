
#import <Cocoa/Cocoa.h>
#import <PostgresServerKit/PostgresServerKit.h>
#import "AppDelegate.h"

@interface HostAccessDelegate : NSObject {
	NSWindow* ibMainWindow;
	NSWindow* ibHostAccessWindow;
	AppDelegate* ibAppDelegate;
	NSArrayController* ibArrayController;	
	NSIndexSet* selectedIndexes;
	NSMutableArray* tuples;
}

// IB Outlets
@property (assign) IBOutlet NSWindow* ibMainWindow;
@property (assign) IBOutlet NSWindow* ibHostAccessWindow;
@property (assign) IBOutlet AppDelegate* ibAppDelegate;
@property (assign) IBOutlet NSArrayController* ibArrayController;

// properties
@property (readonly) FLXPostgresServer* server;
@property (readonly) BOOL canRemoveSelectedTuple;
@property (readonly) FLXPostgresServerAccessTuple* selectedTuple;
@property (readonly) NSUInteger selectedTupleIndex;
@property (retain) NSMutableArray* tuples;

// bindings
@property (assign) NSIndexSet* selectedIndexes;

// IB Actions
-(IBAction)doHostAccess:(id)sender;
-(IBAction)doButton:(id)sender;
-(IBAction)doRemoveTuple:(id)sender;
-(IBAction)doInsertTuple:(id)sender;

@end
