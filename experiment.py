#!/usr/bin/env python3
from mydbtest import *


#Temporarily hardcoded
pairs = {'zgcueqpwhfxsnaasbhixdnehnodnuvixzdnimqetegolmcyhbpefrlownuqlccmc': 
         'kfgwarjkyhntqtomxxjbshercozsxrplsulndyabcmpovtyxpaqhkqbyybuoddigwipjfsrjlad',

         'fvrmdynxvfntfhriapeqqrmqgoxgthtgerrqcgmfewpvlrrzzbstrtrxlmyvuxjjtjarumynzxgwvwobzlj':
         'dxtbzsszxvzokkqlarkrxhyqadvghjidecbhrnatgnbnfbeykxciorcmtoenftdmzergtetpcwqxjawtwbr',

         'dalpgicfhxaflrdfgjjmtzzpxqzrreniqnoigdqdfcvlvmikkbnujxnkayfizrrxxwdyfivdyykrtwgznscjrdfjmcaaiqmauamysghbdhrjwvdlrme':
         'yxwkffkavcuywdryewipqgwyxxmeolqyevwvicsdegeuziewtaktbyrmtefzbrwdisorxcirirq',

         'ltuawoyrlapcmktfgkvisdvnrxwaleygidksikdeqfayhnvxrtixwhynzcylqxliqqxf':
         'nzrntdhqzxqtjgurxmwnnvhlmlrwbqrsxpoxzccgesjwuirrnjeamyjhkfapvbwkqijjhhmjufkcbwoqxjsfuqkoxibsyxfaftxoeqimvncdgubtsuxypd' }


def querytest( database, indexfile, datatype ):

    #Query 1 Test
    total_time = 0
    for key in pairs:
        total_time += get_withKey( database, key )
    avg1 = total_time/len(pairs)

    #Query 2 Test
    total_time = 0
    for value in pairs.values():
        total_time += get_withData( database, indexfile, value )
        print("Total time:", total_time)
    avg2 = total_time/len(pairs)

    #Query 3 Test
    total_time = 0
    rangelist = [('a','ab'),('f','fb'),('m','mb'),('r','rb')]
    for low_value, high_value in rangelist:
        total_time += get_withRange( database, datatype, low_value, high_value )
    avg3 = total_time/len(rangelist)
    return avg1,avg2,avg3

def main():
    #BTREE
    datatype = (db.DB_BTREE, None)
    database, indexfile = create_DB( datatype )
    results_btree = querytest( database, indexfile, datatype )
    demolish_DB( database, indexfile )

    #HASH
    datatype = (db.DB_HASH, None)
    database, indexfile = create_DB( datatype )
    results_hash = querytest( database, indexfile, datatype )
    demolish_DB( database, indexfile )

    #INDEXFILE
    datatype = (db.DB_BTREE, db.DB_BTREE)
    database, indexfile = create_DB( datatype )
    results_indexfile = querytest( database, indexfile, datatype )
    demolish_DB( database, indexfile )

    print()
    print("Btree")
    print("Query 1,2,3 time:", results_btree)
    print("Hash")
    print("Query 1,2,3 time:", results_hash)
    print("Indexfile")
    print("Query 1,2,3 time:", results_indexfile)
    print()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print()
        print( "Sudden close!\nForcing removal of database..." )
        demolish_DB( None, None )
        print( "Exit was successful" )
