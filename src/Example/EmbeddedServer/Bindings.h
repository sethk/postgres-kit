
#import <Cocoa/Cocoa.h>

@interface Bindings : NSObject {
	IBOutlet NSWindow* ibMainWindow;
	IBOutlet NSWindow* ibSelectWindow;
	IBOutlet NSTextView* ibOutput;
	IBOutlet NSTextField* ibInput;
	
	NSArray* databases;
}

@property (retain) NSArray* databases;

-(NSWindow* )mainWindow;
-(NSWindow* )selectWindow;
-(void)clearOutput;
-(void)appendOutputString:(NSString* )theString color:(NSColor* )theColor bold:(BOOL)isBold;
-(void)setInputEnabled:(BOOL)isEnabled;
-(NSString* )inputString;
-(void)setInputString:(NSString* )theString;

@end
