from bsddb3 import db

randomDict = {
    "a":"1",
    "b":"2",
    "c":"3",
    "d":"4",
    "e":"5",
    "f":"6",
    "g":"7",
    "h":"8",
    "i":"4"     }
    
def callback( key, data ):
    print( key, data )
    return data
    
DATABASE_NAME = 'fruits.db'
    
primaryDB = db.DB()
primaryDB.open( DATABASE_NAME, "primary", db.DB_BTREE, db.DB_CREATE )

secDB = db.DB()
secDB.set_flags( db.DB_DUP )
secDB.set_get_returns_none(2)
secDB.open( DATABASE_NAME, "secondary", db.DB_HASH, db.DB_CREATE )

primaryDB.associate( secDB, callback )

for key in randomDict:
    value = randomDict[key].encode( encoding="UTF-8" )
    key = key.encode( encoding="UTF-8" )
    primaryDB.put( key, value )
   
cur = secDB.cursor( None )   
vals = cur.pget( key=b'4', flags=db.DB_SET )
while vals != None:
    print(vals)
    vals = cur.pget( db.DB_NEXT_DUP )