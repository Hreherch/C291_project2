#!/usr/bin/env python3
import sys
import os
import random
from time import time
from bsddb3 import db

# Constants for bsddb3 usage/generation
DATABASE = "/tmp/doupton_db/dbdb.db"
INDEXFILE = "/tmp/doupton_db/index.db"
DB_SIZE = 100000
SEED = 1000000


#=============================================================================
# Function: create_DB
#=============================================================================
# ARGUMENTS:
#       datatype:   A tuple of database types for the databases, selected by
#                   the user when the program started.
#
# ABOUT:
#       Opens database(s) for use with the program. Then calls populate to
#   put the randomized/seeded entries into the database. 
#
#   Returns the populated databases.
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
#       database:   The database that is to be populated by this function
#
# ABOUT:
#       Uses the sample python3.py code provided to populate the given 
#   database with the same seed.
#
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

            
#=============================================================================
# Function: Takes args and does thing
#=============================================================================
# ARGUMENTS:
#       outputFile: The file pointer to "answers" where gotten key:value pairs
#                   will be written.
#
#       keys:       A list of UTF-8 encoded keys.
#
#       datum:      A list of UTF-8 encoded values.
#
# ABOUT:
#       Writes all the give key:value pairs to the given outputFile.
#
def writeAnswer( outputFile, keys, datum ):
    for index in range( len(keys) ):
        outputFile.write( keys[index].decode( "UTF-8" ) + "\n" )
        outputFile.write( datum[index].decode( "UTF-8" ) + "\n\n" )
    
    
#=============================================================================
# Function: get_withKey
#=============================================================================
# ARGUMENTS:
#       outputFile: The file pointer to write answers to.
#
#       database:   The database to be searched for the key.
#
#       key:        Not needed for running, if specified will allow user to 
#                   bypass stdin method of key entry (i.e. for testing 
#                   purposes).
#
# ABOUT:
#       Gets the data from the created database (from create_DB) and reports
#   the amount of data/time it took to do this. 
#
#   Returns the time it took for the search to run
#
def get_withKey( outputFile, database, key=None ):
    keys = []
    datum = []
    
    # If not called with a key directly, ask for key via stdin
    if not key:
        key = input( "Enter the key: " ).lower()
    key = key.encode( "UTF-8" )

    record_count = 0
    print( "Retrieving data..." )
    cursor = database.cursor()

    start = time()
    result = cursor.get( key, None, flags=db.DB_SET )
    if result:
        record_count += 1
        keys.append( result[0] )
        datum.append( result[1] )

    total_time = round( ( time() - start ) * 10e6 ) 
    cursor.close()
    
    # If there was an answer, write it to the output file.
    if result:
        writeAnswer( outputFile, keys, datum )
        
    print( "Number of Records:", record_count )
    print( "Time Elapsed (micro seconds)", total_time )
    return total_time
 
 
#=============================================================================
# Function: get_withData
#=============================================================================
# ARGUMENTS:
#       outputFile: The file pointer to write answers to.
#
#       database:   The database to be searched for the data.
#
#       indexfile:  If there is an indexfile attached to the database, pass it
#                   as such.
#
#       value:      Not needed for running, if specified will allow user to 
#                   bypass stdin method of data entry (i.e. for testing 
#                   purposes).
#
# ABOUT:
#       Checks which keys match the data that are given, ensures that the 
#   returned values are written to answers.
#
#   Returns the time it took for the search to run
#
def get_withData( outputFile, database, indexfile, value=None ):
    keys = []
    datum = []
    
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
            keys.append( result[1] )
            datum.append( result[0] )
            result = cursor.pget( db.DB_NEXT_DUP )
    else:
        cursor = database.cursor()
        start = time()
        result = cursor.first()
        while result:
            if result[1] == value:
                record_count += 1
                keys.append( result[0] )
                datum.append( result[1] )
            result = cursor.next()

    total_time = round( ( time() - start ) * 10e6 )
    cursor.close()
    writeAnswer( outputFile, keys, datum )
    print( "Number of Records:", record_count )
    print( "Time Elapsed (micro seconds)", total_time )
    return total_time

    
#=============================================================================
# Function: get_withRange
#=============================================================================
# ARGUMENTS:
#       outputFile: The file pointer to write answers to.
#
#       database:   The database to be searched for the data.
#
#       indexfile:  If there is an indexfile attached to the database, pass it
#                   as such.
#
#       low_value:  Not needed for running, if specified will allow user to 
#                   bypass stdin method of range entry (i.e. for testing 
#                   purposes).
#
#       high_value: see low_value
#
# ABOUT:
#   
#
#   Returns the time it took for the search to run
#
def get_withRange( outputFile, database, datatype, \
                   low_value=None, high_value=None ):
    keys = []
    datum = []
    
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
            keys.append( result[0] )
            datum.append( result[1] )
            result = cursor.next()

    elif datatype[0] == db.DB_HASH:
        start = time()
        result = cursor.first()
        while result:
            if low_value <= result[0] <= high_value:
                record_count += 1
                keys.append( result[0] )
                datum.append( result[1] )
            result = cursor.next()

    total_time = round( ( time() - start ) * 10e6 )
    cursor.close()
    writeAnswer( outputFile, keys, datum )
    print( "Number of Records:", record_count )
    print( "Time Elapsed (micro seconds)", total_time )
    return total_time
 
 
#=============================================================================
# Function: demolish_DB
#=============================================================================
# ARGUMENTS:
#       database:   The main db.DB() object used to store the key/value pairs
#
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
        # Since INDEXFILE is not always used, do not print unless specified
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
# Function: get_datatype 
#=============================================================================
# ARGUMENTS:
#       N/A
#
# ABOUT:
#       Based on a user's input argument (i.e. hash, btree...), returns the DB
#   type(s) as a tuple associated with their choice for use with create_DB. 
#   Tuple format: (primary datatype, secondary datatype)
#
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
#       new: True if you want to prompt the user to press enter before
#            returning to the window that will be printed
#
# ABOUT:
#       Prints a menu that shows the user their options with mydbtest.
#
def showoptions( new=False ):
    if not new:
        input( "Press enter to return to menu\n" )
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
#       outputFile: The text file used to show key:value answer pairs after
#                   the run of the program.
#
# ABOUT:
#       The main function for mydbtest.py, prints out a menu of options and
#   takes user input to aid the user getting from various functions easily.
#
def main():
    datatype = get_datatype()
    outputFile = open( "answers", 'w' )
    database = None
    indexfile = None
    
    showoptions( True )
    
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
            get_withKey( outputFile, database )
        
        # Get with DATA
        elif option == '2':
            if not database:
                errMsg = "ERROR:\tYou must first create the database."
                print( errMsg )
                continue
            get_withData( outputFile, database, indexfile )
        
        # Get with RANGE
        elif option == '3':
            if not database:
                errMsg = "ERROR:\tYou must first create the database."
                print( errMsg )
                continue
            get_withRange( outputFile, database, datatype )
            
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
    
    # Clears the terminal when a user exits
    os.system( "clear" )
    outputFile.close()
 
 
# Runs main as try/except, this allows us to cull the database properly
# in the event of a keyboard interrupt.
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print()
        print( "Sudden close!\nForcing removal of database..." )
        demolish_DB( None, None )
        outputFile.close()
        print( "Exit was successful" )
