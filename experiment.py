#!/usr/bin/env python3
from mydbtest import *

#Random test keys
pairs = { 'mfvpyebviinuluphgqfqgiygfdcjxbccgtfvplydpbmzorqxqaxeqglyyzxbeoeouclbddlhyvy':
          'wecghbenhpygzugmsvgazoulooadcentkpydvxhjavgoevvvczztohydmyzhnlwcdjrdxcubclkfhcmffoujdqqxczxnt',

          'tkupbpcyldcfzfcqlxcrtaqfjfawftvflqfbeyguxdunksxhodzkfryobcwgpvzwtgyxzhtizpulmikgnltjrauofxmllbuakpkuppoklrqeymeocplriehfb':
          'wuwcehqqbskaboxtvwmngiexgajwcfbrlshrqsugsboqvitouuorrvxxkcohxsbvskpgoydefmg',

          'soicnchqepbhyhezmjqchyebhvulbgxyoqtvqlfyjgafpyilzsbasjpiijgytfjdeqxcroudsijbftpaqbtjsiuqvenoouastshmdtqqpujukpednlsghgpzumyvox':
          'iqgpadwvkdkmuderpkwouiewgzjrdfjjanrvrtiozuqeejodrbqtgauygbcwjxbgmaqg',

          'xgmbjxrifwqpujffxzmqhouhzbnfsisheemcjoxvfzuzpewmppffnlakmkndjronxhhqksvzgzheyjvnwynds':
          'pdlszbiqpkfwwftxfhafnenpjqbqhagkqhhxwgmxomciileyhjtxuhweycddhwswazk' }


rangelist = [('a','ab'),('f','fb'),('m','mb'),('r','rb')]

#Execute each query one by one
def querytest( outputFile, database, indexfile, datatype ):

    #Query 1 Test
    total_time = 0
    for key in pairs:
        total_time += get_withKey( outputFile, database, key )
    avg1 = str(round(total_time/len(pairs)))

    #Query 2 Test
    total_time = 0
    for value in pairs.values():
        total_time += get_withData( outputFile, database, indexfile, value )
    avg2 = str(round(total_time/len(pairs)))

    #Query 3 Test
    total_time = 0
    for low_value, high_value in rangelist:
        total_time += get_withRange( outputFile, database, datatype, \
                                     low_value, high_value )
    avg3 = str(round(total_time/len(rangelist)))
    return avg1,avg2,avg3

def main():
    outputFile = open( "answers", 'w' )
    #BTREE
    datatype = (db.DB_BTREE, None)
    database, indexfile = create_DB( datatype )
    results_btree = querytest( outputFile, database, indexfile, datatype )
    demolish_DB( database, indexfile )

    #HASH
    datatype = (db.DB_HASH, None)
    database, indexfile = create_DB( datatype )
    results_hash = querytest( outputFile, database, indexfile, datatype )
    demolish_DB( database, indexfile )

    #INDEXFILE
    datatype = (db.DB_BTREE, db.DB_HASH)
    database, indexfile = create_DB( datatype )
    results_indexfile = querytest( outputFile, database, indexfile, datatype )
    demolish_DB( database, indexfile )

    outputFile.close()

    #Print results
    print()
    print(" Time (\u00b5s) | Query 1  Query 2  Query 3")
    print("-----------+---------------------------")
    print("Btree      |", results_btree[0].rjust(7),
                         results_btree[1].rjust(8),
                         results_btree[2].rjust(8))
    print("Hash       |", results_hash[0].rjust(7),
                         results_hash[1].rjust(8),
                         results_hash[2].rjust(8))
    print("Indexfile  |", results_indexfile[0].rjust(7),
                         results_indexfile[1].rjust(8),
                         results_indexfile[2].rjust(8))
    print()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt: #Protect against Ctrl-C
        print()
        print( "Sudden close!\nForcing removal of database..." )
        demolish_DB( None, None )
        print( "Exit was successful" )
