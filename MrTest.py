import os
import sys
import time
import random

# ensures a valid test type for testing
def checkARGV( change=None ):
    if not change:
        if len( sys.argv ) < 2:
            print( "You need to specify the test type." )
            exit()
        type = sys.argv[1].strip()
    else:
        type = change
    
    types = [ "btree", "hash", "indexfile" ]
    
    for choice in types:
       if type == choice:
            return type
            
    print( "You must choose one of: ", end="" )
    for choice in types:
        if choice == types[-1]:
            print( choice )
            print()
            return False
        else:
            print( choice, end=", " )
    

# creates the DB as a dictionary in Python
def startup():
    dict = {}
    txt = open( "key.txt", 'r' )
    for line in txt:
        keyval = line.strip().split(":")
        dict[keyval[0]] = keyval[1]
    txt.close()
    return dict

# removes test.txt from the directory
def cleanTestFile():
    os.system( "rm -f input.txt" )
    os.system( "rm -f test.txt" )
    
# generates 100 key tests
# not including initial db setup
def generateKeyTest( outputFile, dbDict ):
    for i in range( 100 ):
        outputFile.write( "2\n" )
        keyVal = dbDict.popitem()
        outputFile.write( keyVal[0] + "\n" )
        outputFile.write( "\n" )
    
# tests key retrevial
def keyTest( type, dbDict ):
    cleanTestFile()
    
    print( "generating test output..." )
    
    testFile = open( "test.txt", 'w' )
    testFile.write( "1\n" )
    testFile.write( "\n" )
    generateKeyTest( testFile, dbDict )
    testFile.write( "5\n" )
    testFile.write( "\n" )
    testFile.write( "6" )
    testFile.close()
    
    print( "generation complete." )
    
    print( "running test..." )
    
    sysStr = "./mydbtest.py " + type + " < test.txt > output.txt"
    os.system( sysStr )
    
    print( "test complete." )
    
    print( "reading test..." )
    
    readTest = open( "output.txt", 'r' )
    count = 0
    totalTime = 0
    for line in readTest:
        if "Time Elapsed" in line:
            line = line.strip()
            time = line.split()[-1]
            time = int( time )
            totalTime += time
            count += 1
    readTest.close()
            
    print( "read complete!" )
    
    print()
    print( "processed", count, "tests..." )
    print( "total time for all 100:", totalTime )
    print( "average time:", totalTime / count )
    
    
def generateDataTest( outputFile, dbDict ):
    for i in range( 100 ):
        outputFile.write( "3\n" )
        keyVal = dbDict.popitem()
        outputFile.write( keyVal[1] + "\n" )
        outputFile.write( "\n" )
        
    
def dataTest( type, dbDict ):
    cleanTestFile()
    
    print( "generating test output..." )
    
    testFile = open( "test.txt", 'w' )
    testFile.write( "1\n" )
    testFile.write( "\n" )
    generateDataTest( testFile, dbDict )
    testFile.write( "5\n" )
    testFile.write( "\n" )
    testFile.write( "6" )
    testFile.close()
    
    print( "generation complete." )
    
    print( "running test..." )
    
    sysStr = "./mydbtest.py " + type + " < test.txt > output.txt"
    os.system( sysStr )
    
    print( "test complete." )
    
    print( "reading test..." )
    
    readTest = open( "output.txt", 'r' )
    count = 0
    totalTime = 0
    for line in readTest:
        if "Time Elapsed" in line:
            line = line.strip()
            time = line.split()[-1]
            time = int( time )
            totalTime += time
            count += 1
    readTest.close()
    
    print( "read complete!" )
    
    print()
    print( "processed", count, "tests..." )
    print( "total time for all 100:", totalTime )
    print( "average time:", totalTime / count )
    
    
def generateRangeTest( outputFile, keyList ):
    numEntries = 0
    numRanges = 0
    for i in range(100):
        loc = random.randint( 0, len(keyList)-201 )
        plus = random.randint( 100, 200 )
        start = keyList[ loc ]
        end = keyList[ loc + plus ]
        outputFile.write( "4\n" )
        outputFile.write( start + "\n" )
        outputFile.write( end + "\n" )
        outputFile.write( "\n" )
        
        numEntries += plus + 1
        numRanges += 1
        
    return numRanges, numEntries    
        
    
def rangeTest( type, dbDict ):
    cleanTestFile()
    
    print( "generating test output..." )
    
    keyList = list( dbDict.keys() )
    keyList.sort()

    testFile = open( "test.txt", 'w' )
    testFile.write( "1\n" )
    testFile.write( "\n" )    
    numRanges, numEntries = generateRangeTest( testFile, keyList )
    testFile.write( "5\n" )
    testFile.write( "\n" )
    testFile.write( "6" )
    testFile.close()
    
    print( "generated", numRanges, "ranges with", numEntries, "keys" )
    print( "generation complete." )
    
    print( "running test..." )
    
    sysStr = "./mydbtest.py " + type + " < test.txt > output.txt"
    os.system( sysStr )
    
    print( "test complete." )
    
    print( "reading test..." )
    
    readTest = open( "output.txt", 'r' )
    count = 0
    totalTime = 0
    for line in readTest:
        if "Time Elapsed" in line:
            line = line.strip()
            time = line.split()[-1]
            time = int( time )
            totalTime += time
            count += 1
    readTest.close()
    
    print( "read complete!" )
    
    print()
    print( "processed", count, "tests..." )
    print( "total time for all 100:", totalTime )
    print( "average time:", totalTime / count )
    
def superRead( inputFile ):
    count = 0
    totalTime = 0
    for line in inputFile:
        if "Time Elapsed" in line:
            line = line.strip()
            time = line.split()[-1]
            time = int( time )
            totalTime += time
            count += 1
            if count == 100:
                return count, totalTime
    return count, totalTime
   
    
def generateSuper( outputFile, keyList, dbDict ):
    generateKeyTest( outputFile, dbDict )
    generateKeyTest( outputFile, dbDict )
    generateKeyTest( outputFile, dbDict )
    generateKeyTest( outputFile, dbDict )
    generateDataTest( outputFile, dbDict )
    generateDataTest( outputFile, dbDict )
    generateDataTest( outputFile, dbDict )
    generateDataTest( outputFile, dbDict )
    generateRangeTest( outputFile, keyList )
    generateRangeTest( outputFile, keyList )
    generateRangeTest( outputFile, keyList )
    generateRangeTest( outputFile, keyList )
    
def superCrunch( fileName ):
    keyCount = 0
    keyTime = 0
    dataCount = 0
    dataTime = 0
    rangeCount = 0
    rangeTime = 0
    
    for i in range(4):
        c, t = superRead( fileName )
        keyCount += c
        keyTime += t
    keyTime /= 4
    for i in range(4):
        c, t = superRead( fileName )
        dataCount += c
        dataTime += t
    dataTime /= 4
    for i in range(4):
        c, t = superRead( fileName )
        rangeCount += c
        rangeTime += t
    rangeTime /= 4
    
    return [keyTime, dataTime, rangeTime]
    
    
def superTest( dbDict ):
    cleanTestFile()
    print( "generating test output..." )
    testFile = open( "test.txt", 'w' )
    testFile.write( "0\n" )
    testFile.write( "\n" ) 
    
    keyList = list( dbDict.keys() )
    keyList.sort()
    
    generateSuper( testFile, keyList, dbDict )
    
    testFile.write( "4\n" )
    testFile.write( "\n" )
    testFile.write( "5" )
    testFile.close()
    
    print( "generation complete." )
    
    # create hash DB ========================================================
    type = "hash"
    print( "HASH run..." )
    
    print( "running test..." )
    
    sysStr = "./mydbtest.py " + type + " < test.txt > output.txt"
    os.system( sysStr )
    
    print( "test complete." )
    
    print( "crunching numbers..." )

    readTest = open( "output.txt", 'r' )    
    hash = superCrunch( readTest )
    readTest.close()
    
    print( "HASH complete..." )
    
    # create btree DB ======================================================
    type = "btree"
    print( "BTREE run..." )
    
    print( "running test..." )
    
    sysStr = "./mydbtest.py " + type + " < test.txt > output.txt"
    os.system( sysStr )
    
    print( "test complete." )
    
    print( "crunching numbers..." )

    readTest = open( "output.txt", 'r' )    
    btree = superCrunch( readTest )
    readTest.close()
    
    print( "BTREE complete..." )
    
    
    # create indexfile DB ======================================================
    type = "indexfile"
    print( "INDEX run..." )
    
    print( "running test..." )
    
    sysStr = "./mydbtest.py " + type + " < test.txt > output.txt"
    os.system( sysStr )
    
    print( "test complete." )
    
    print( "crunching numbers..." )

    readTest = open( "output.txt", 'r' )    
    indexfile = superCrunch( readTest )
    readTest.close()
    
    print( "INDEX complete..." )
    
    print( hash )
    print( btree )
    print( indexfile )
    
def main():
    type = checkARGV()
    if not type:
        exit()
        
    dbDict = startup()
   
    while True:
        print()
        print( len(dbDict), "keys in dbDict" )
        print( "testing on type=" + type )
        print()
        print( "[1]: Test keys" )
        print( "[2]: Test datas" )
        print( "[3]: Test ranges" )
        print( "[4]: Refresh dbDict" )
        print( "[5]: Change test type")
        #print( "[7]: supertest" )
        print( "[9]: Exit")
        choice = input( "MrTest>" )
        if choice == '1':
            keyTest( type, dbDict )
        elif choice == '2':
            dataTest( type, dbDict )
        elif choice == '3':
            rangeTest( type, dbDict )
        elif choice == '4':
            dbDict = startup()
        elif choice == '5':
            while True:
                type = input( "enter new type: " )
                type = checkARGV( type )
                if type:
                    break
        #elif choice == '7':
        #    superTest( dbDict )
        elif choice == '9':
            exit()
        else:
            print( "please make a valid choice." )

            
            
main()
