#! /usr/bin/env python3

# Global Macro for terminal sized new line feed.
CLR_SCRN = chr(27) + "[2J"

# =============================================================================
# class:
# =============================================================================
#
#
class menuWidget:
    def __init__( self, title="menuWidget", prompt=">",
                  options=[], gotos=[], now=False       ):
                  
        # Ensure that every option has a related function.
        if len( options ) != len( gotos ):
            errMsg = "There must be the same number of options as gotos."
            raise Exception( errMsg )
            
        self.options = options
        self.gotos = gotos
        self.prompt = prompt
        self.title = title
        self.error = ""
        self.info = ""
        
        if now:
            self.wait()
     
    def __showHeader( self ):
        print( "=" * 80 )
        print( " =" * 40 )
        print( self.title.center( 80, " " ) ) 
        print( "= " * 40 )
        print()
         
    def __showMenu( self ):
        #print( CLR_SCRN )
        self.__showHeader()
        if len( self.error ) > 0:
            print( "ERROR!".center( 80 ) )
            print( self.error.center( 80 ) )
            self.error = ""
            print()
            
        if len( self.info ) > 0:
            print( "INFO:".center( 80 ) )
            print( self.info.center( 80 ) )
            self.info = ""
            print() 
            
        
    def __verify( self ):
        # Ensure the option is an integer
        try:
            self.grab = int( self.grab )
        except:
            self.error = "Entry must be an integer!"
            return False
        
        if ( self.grab < 0 ) or ( self.grab >= len( self.options ) ):
            self.error = "You must choose one of the options specified!"
            return False
            
        return True
        
    def wait( self ):
        print( "test" )
        while( True ):
            self.__showMenu()
            print( "Enter your option:" )
            self.grab = input( self.prompt )
            if not self.__verify():
                continue
            else:
                self.gotos[ self.grab ]( self )
        
        
        

# =============================================================================
# Function:
# =============================================================================
#
#
def create_DB( menu ):
    menu.info = "create_DB called..." 

# =============================================================================
# Function:
# =============================================================================
#
#    
def get_withKey( menu ):
    menu.info = "get_withKey called..."
 
# =============================================================================
# Function:
# =============================================================================
#
# 
def get_withData( menu ):
    menu.info = "get_withData called..." 

# =============================================================================
# Function:
# =============================================================================
#
#    
def get_withRange( menu ):
    menu.info = "get_withRange called..." 
    while( True ):
        # present data, wait here
        a = input()
        # "Hit enter to go back to main menu"

# =============================================================================
# Function:
# =============================================================================
#
#
def demolish_DB( menu ):
    menu.info = "create_DB called..." 

# =============================================================================
# Function:
# =============================================================================
#
#    
def main():
    print( "Initializing DBDB..." + CLR_SCRN )
    options = [ "Create/Populate DB", "Get With Key", "Get With Data", "Get With Range", "Destroy DB", "Quit" ]
    gotos = [ create_DB, get_withKey, get_withData, get_withRange, demolish_DB, quit ]
    DBDB = menuWidget( options=options, gotos=gotos, now=True )
    DBDB.wait()
    DBDB.__init__()
    
# if this file is called directly, will run main()   
if __name__ == "__main__":
    main()