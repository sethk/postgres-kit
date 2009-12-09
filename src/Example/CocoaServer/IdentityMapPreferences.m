
#import "IdentityMapPreferences.h"

@implementation IdentityMapPreferences

////////////////////////////////////////////////////////////////////////////////

@synthesize ibMainWindow;
@synthesize ibIdentityMapWindow;
@synthesize ibAppDelegate;
@synthesize ibGroupsArrayController;
@synthesize ibIdentityArrayController;
@dynamic server;

////////////////////////////////////////////////////////////////////////////////
// constructors

-(void)awakeFromNib {
	[[self ibGroupsArrayController] addObserver:self forKeyPath:@"selectionIndexes" options:NSKeyValueObservingOptionNew context:nil];
}

////////////////////////////////////////////////////////////////////////////////
// properties

-(FLXPostgresServer* )server {
	return [FLXPostgresServer sharedServer];
}

////////////////////////////////////////////////////////////////////////////////
// private methods

-(void)identityMapDidEnd:(NSWindow *)sheet returnCode:(NSInteger)returnCode contextInfo:(void *)contextInfo {
	[sheet orderOut:self];	
	
	if(returnCode==NSOKButton) {
		NSLog(@"TODO: Write identity map");
	}
}

-(void)groupDidChange:(NSString* )theGroup {
	// hide objects in identity content array
	NSLog(@"TODO: Load group %@",theGroup);	
}

-(void)observeValueForKeyPath:(NSString *)keyPath ofObject:(id)object change:(NSDictionary *)change context:(void *)context {
	if([keyPath isEqual:@"selectionIndexes"] && [object isEqual:[self ibGroupsArrayController]]) {
		if([[[self ibGroupsArrayController] selectedObjects] count]==1) {
			NSDictionary* theGroup = [[[self ibGroupsArrayController] selectedObjects] objectAtIndex:0];
			NSParameterAssert([theGroup isKindOfClass:[NSDictionary class]]);
			[self groupDidChange:[theGroup objectForKey:@"group"]];
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

-(IBAction)doIdentityMap:(id)sender {	
	// read tuples
	NSArray* theTuples = [[self server] readIdentityTuples];

	// create array of groups
	NSMutableArray* theGroups = [NSMutableArray array];
	for(FLXPostgresServerIdentityTuple* theTuple in theTuples) {
		NSString* theGroup = [theTuple group];
		BOOL isSupergroup = [theTuple isSupergroup];
		if([theGroups containsObject:theGroup]==NO) {
			NSMutableDictionary* theDictionary = [NSMutableDictionary dictionary];
			[theDictionary setObject:theGroup forKey:@"group"];
			[theDictionary setObject:[NSNumber numberWithBool:isSupergroup] forKey:@"isSupergroup"];
			[theDictionary setObject:(isSupergroup ? [NSColor grayColor] : [NSColor blackColor]) forKey:@"textColor"];
			[theGroups addObject:theDictionary];
		}
	}
	
	// add groups to array controller
	[[self ibGroupsArrayController] setContent:theGroups];
	// add tuples to array controller
	[[self ibIdentityArrayController] setContent:theTuples];
	
	// begin display	
	[NSApp beginSheet:[self ibIdentityMapWindow] modalForWindow:[self ibMainWindow] modalDelegate:self didEndSelector:@selector(identityMapDidEnd:returnCode:contextInfo:) contextInfo:nil];
}

-(IBAction)doButton:(id)sender {
	NSButton* theButton = (NSButton* )sender;
	NSParameterAssert([theButton isKindOfClass:[NSButton class]]);
	
	if([[theButton title] isEqual:@"OK"]) {
		[NSApp endSheet:[self ibIdentityMapWindow] returnCode:NSOKButton];
	} else {
		[NSApp endSheet:[self ibIdentityMapWindow] returnCode:NSCancelButton];
	}
}

@end
