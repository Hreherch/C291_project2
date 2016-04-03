#!/usr/bin/env python3
import sys
import os
import random
from time import time
from bsddb3 import db

DATABASE = "/tmp/doupton_db/dbdb.db"
INDEXFILE = "/tmp/doupton_db/index.db"
DB_SIZE = 100000
SEED = 1000000

#=============================================================================
# Function: Takes args and does thing
#=============================================================================
# ARGUMENTS:
#
# ABOUT:
#

def create_DB( datatype ):
    print("Creating Database...") 

    #Open main database
    database = db.DB()
    try:
        database.open( DATABASE, "Primary", \
                       datatype[0], db.DB_CREATE )
    except Exception as e:
        print("Error occurred when opening database: Err 01")
        print(e)
        return None, None

    indexfile = None
    if datatype[1]:
        #Open indexfile if specified
        indexfile = db.DB()
        indexfile.set_flags(db.DB_DUP) 
        try: 
            indexfile.open( INDEXFILE, "IndexFile", \
                              datatype[1], db.DB_CREATE )
        except Exception as e:
            print("Error occurred when opening indexfile: Err 02")
            print(e)
            database.close()
            return None, None

        #Associate indexfile to main database
        try:
            database.associate( indexfile, lambda key,data: data )
        except Exception as e:
            print("Error occurred when associating indexfile: Err 03")
            print(e)
            indexfile.close()
            database.close()
            return None, None

    populate( database )
    print("|~~~~~~Success!~~~~~~|")
    return database, indexfile

#=============================================================================
# Function: Takes args and does thing
#=============================================================================
# ARGUMENTS:
#
# ABOUT:
#
#Code modified from sample code in notes
def populate( database ):
    print("Populating Database...")

    #Progrss bar tells user how much has been loaded
    progressbar = list("|" + "----loading-data----" + "|\r")
    count = 0
    random.seed(SEED)

    while count < DB_SIZE:
        if count%(DB_SIZE/20) == 0: #Print progressbar
            index = round((20*count/DB_SIZE)) + 1
            if progressbar[index].isalpha():
                progressbar[index] = progressbar[index].upper()
            else:
                progressbar[index] = "|"
            sys.stdout.write("".join(progressbar))
            sys.stdout.flush()
        count += 1

        #Generate Key/Value pair
        krng = 64 + random.randint( 0, 63 )
        key = ""
        for i in range(krng):
            key += str( chr( 97 + random.randint( 0, 25 ) ) )
        vrng = 64 + random.randint( 0, 63 )
        value = ""
        for i in range(vrng):
            value += str( chr( 97 + random.randint( 0, 25 ) ) )
        key = key.encode( "UTF-8" )
        value = value.encode( "UTF-8" )

        #Add to database
        try:
            database.put( key, value, flags=db.DB_NOOVERWRITE )
        except db.DBKeyExistError as e:
            #Try another key that isn't a duplicate
            print("\rDuplicate encountered; Trying different key...")
            sys.stdout.write("".join(progressbar))
            sys.stdout.flush()
            count -= 1
    print(key)

    
#=============================================================================
# Function:
#=============================================================================
# ARGUMENTS:
#
# ABOUT:
#
def get_withKey( database, key=None ):
    if not key:
        key = input( "Enter the key: " ).lower()
    key = key.encode( "UTF-8" )

    record_count = 0
    print( "Retrieving data..." )
    cursor = database.cursor()

    start = time()
    if cursor.get( key, None, flags=db.DB_SET ):
        record_count += 1

    total_time = round( ( time() - start ) * 10e6 )
    cursor.close()
    print( "\nNumber of Records:", record_count )
    print( "Time Elapsed (micro seconds)", total_time )
    return total_time
 
#=============================================================================
# Function:
#=============================================================================
# ARGUMENTS:
#
# ABOUT:
#
def get_withData( database, indexfile, value=None ):
    if not value:
        value = input( "Enter the data: " ).lower()

    value = value.encode( "UTF-8" )
    record_count = 0
    print( "Retrieving data..." )

    if indexfile != None: #If we have an indexfile
        cursor = indexfile.cursor()
        start = time()
        result = cursor.pget( value, None, flags=db.DB_SET )
        while result:
            record_count += 1
            result = cursor.pget( db.DB_NEXT_DUP )
    else:
        cursor = database.cursor()
        start = time()
        result = cursor.first()
        while result:
            if result[1] == value:
                record_count += 1
            result = cursor.next()

    total_time = round( ( time() - start ) * 10e6 )
    cursor.close()
    print( "\nNumber of Records:", record_count )
    print( "Time Elapsed (micro seconds)", total_time )
    return total_time

#=============================================================================
# Function:
#=============================================================================
# ARGUMENTS:
#
# ABOUT:
#
def get_withRange( database, datatype, low_value=None, high_value=None ):
    if not (low_value and high_value):
        low_value = input( "Enter the low valued key: " ).lower()
        high_value = input( "Enter the high valued key: " ).lower()

    low_value = low_value.encode( "UTF-8" )
    high_value = high_value.encode( "UTF-8" )
    record_count = 0
    print( "Retrieving data..." )
    cursor = database.cursor()

    if datatype[0] == db.DB_BTREE:
        start = time()
        result = cursor.set_range( low_value )
        while result:
            if result[0] > high_value:
                break
            record_count += 1
            result = cursor.next()

    elif datatype[0] == db.DB_HASH:
        start = time()
        result = cursor.first()
        while result:
            if low_value <= result[0] <= high_value:
                record_count += 1
            result = cursor.next()

    total_time = round( ( time() - start ) * 10e6 )
    cursor.close()
    print( "\nNumber of Records:", record_count )
    print( "Time Elapsed (micro seconds)", total_time )
    return total_time
 
#=============================================================================
# Function: demolish_DB
#=============================================================================
# ARGUMENTS:
#       database:   The main db.DB() object used to store the key/value pairs
#       indexfile:  The associated db.DB() object used in the 'indexfile' run 
#                   of mydbtest
#
# ABOUT:
#       demolish_DB uses a db.DB() object to remove an indexfile, if it
#   exists, and the main database file (if it exists). The function will
#   print out appropriate messages depending on the current status of these
#   databases.
#
# WARNING:
#       If there is ever an error with the application, we cannot properly
#   demolish the database and it may be left in the temporary file. 
#
def demolish_DB( database, indexfile, verbose=False ):
    removeDB = db.DB()
    
    # Remove INDEXFILE
    try:
        if indexfile != None:
            indexfile.close()
        removeDB.remove( INDEXFILE )
        print( INDEXFILE, "demolished." )
    except db.DBNoSuchFileError:
        if verbose:
            noDBMsg = "The database at " + INDEXFILE + " does not exist!"
            print( noDBMsg )
    except Exception as e:
        print( e )
        print( "Something unexpected occurred!" )
        print( "Type 'make' to ensure next run is clean!" )
        exit()
    removeDB.close()
    
    removeDB = db.DB()      # closing/reopening is neccessary
    # Remove DATABASE
    try:
        if database != None:
            database.close()
        removeDB.remove( DATABASE )
        print( DATABASE, "demolished." )
    except db.DBNoSuchFileError:
        noDBMsg = "The database at " + DATABASE + " does not exist!"
        print( noDBMsg )
    except Exception as e:
        print( e )
        print( "Something unexpected occurred!" )
        print( "Type 'make' to ensure next run is clean!" )
        exit()  
    removeDB.close()
    
    return None, None

#=============================================================================
# Function: 
#=============================================================================
# ARGUMENTS:
#
# ABOUT:
#
# Returns a tuple of (primary datatype, secondary datatype)
def get_datatype():
    if len( sys.argv ) == 1:
        errMsg = sys.argv[0] + ": No DB type specified\n" +\
                 "Try '" + sys.argv[0] + " help' for more information.\n"
        print( errMsg )
        exit()
    if len( sys.argv ) > 2:
        errMsg = sys.argv[0] + ": too many options specified\n" +\
                 "Try '" + sys.argv[0] + " help' for more information.\n"
        print( errMsg )
        exit()
    datatype = sys.argv[1].lower()
    if datatype == "btree":
        datatype = (db.DB_BTREE, None)
    elif datatype == "hash":
        datatype = (db.DB_HASH, None)
    elif datatype == 'indexfile':
        datatype = (db.DB_BTREE, db.DB_BTREE)
    elif datatype == "help":
        helpMsg = "Usage: 'mydbtest OPTION'\n" +\
                  "options: 'btree', 'hash', 'indexfile', 'help'\n"
        print( helpMsg )
        exit()
    else:
        errMsg = sys.argv[0] + ": invalid option -- '" +\
                 sys.argv[1] + "'\n" +\
                 "Try '" + sys.argv[0] + " help' for more information.\n"
        print( errMsg )
        exit()
    return datatype
    
#=============================================================================
# Function: showoptions
#=============================================================================
# ARGUMENTS:
#
# ABOUT:
#
def showoptions( new=False ):
    if not new:
        input( "\nPress enter to return to menu\n" )
    os.system("clear")

    # dbDB text header 
    print( "\r" + "=" * 80 )
    print( " =" * 40 )
    print( "dbDB".center(80) )
    print( "= " * 40 + "\n" )

    options = [ "Create/Populate Database", "Get with KEY", "Get with DATA", \
                "Get with RANGE", "Demolish Database", "Exit" ]
    
    # prints the above options in a nice format
    for index in range( len( options ) ):
        optStr = "[" + str(index) + "]: " + options[index]
        print( optStr )
    print()


#=============================================================================
# Function: main
#=============================================================================
# ARGUMENTS:
#       N/A
#
# ABOUT:
#
def main():
    datatype = get_datatype()
    database = None
    indexfile = None
    
    showoptions(True)
    # loop forever until exit
    while True:
        option = input("dbDB>")
       
        # Create/Populate Database
        if option == '0':
            if database != None:
                errMsg = "ERROR:\tThere is an active Database already.\n" +\
                         "You cannot populate another without " +\
                         "destroying it first."
                print( errMsg )
                continue
            database, indexfile = create_DB( datatype )
        
        # Get with KEY
        elif option == '1':
            if not database:
                errMsg = "ERROR:\tYou must first create the database."
                print( errMsg )
                continue
            get_withKey( database )
        
        # Get with DATA
        elif option == '2':
            if not database:
                errMsg = "ERROR:\tYou must first create the database."
                print( errMsg )
                continue
            get_withData( database, indexfile )
        
        # Get with RANGE
        elif option == '3':
            if not database:
                errMsg = "ERROR:\tYou must first create the database."
                print( errMsg )
                continue
            get_withRange( database, datatype )
            
        # Demolish Database
        elif option == '4':
            database, indexfile = demolish_DB( database, indexfile )
            
        # Exit
        elif option == '5':
            if database != None:
                errMsg = "ERROR:\tYou are not allowed to quit " +\
                         "without demolishing first."
                print( errMsg )
                continue
            break
            
        # If a proper option was not selected...
        else:
            errMsg = "ERROR:\tYou must specify one of the options listed."
            print( errMsg )
            continue
            
        showoptions()
        
    os.system( "clear" )
    
#        
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print()
        print( "Sudden close!\nForcing removal of database..." )
        demolish_DB( None, None )
        print( "Exit was successful." )
