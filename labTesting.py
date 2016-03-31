# example usage of dsddb3 in Python. 

from bsddb3 import db
DATABASE = 'fruits.db'
database = db.DB()
database.open( DATABASE, None, db.DB_HASH, db.DB_CREATE )
# database.open(DATABASE, None, db.DB_BTREE, db.DB_CREATE )
# database.open(DATABASE, None, db.DB_QUEUE, db.DB_CREATE )
# database.open(DATABASE, None, db.DB_RECNO, db.DB_CREATE )
	# The arguments correspond to (fileName, database 
	# name within the file for multiple databases, 
	# database type, flag to create database) 

curs = database.cursor()

# insertion of (key, data) pair using the cursor
curs.put( b'apple', 'red', db.DB_KEYFIRST )

# insertion using the database object’s put method
database.put( b'pear', 'green' ) 

# use the cursor
iter = curs.first()
while iter:
		print( iter )
		iter = curs.next()

# using the database object’s get method: only retrieves the value
result = database.get( b'pear' )
print( result )
# b‘green’ 

# remove using the cursor 
# deletes the key-data pair currently referenced by the cursor
curs.delete()

# our database now has (apple,red) only
# remove by using database object 
database.delete( b'apple' )

curs.close()
database.close()

# Stuff I can't get ti wirj
# ============================================================================ #
# # declare duplicates allowed before you create the database
# DATABASE = 'moreFruit.db'
# database = db.DB()
# database.set_flags( db.DB_DUP )
# database.open( DATABASE, None, db.DB_HASH, db.DB_CREATE )

# cur = database.cursor()

# # insert duplicates
# cur.put( b'blue', "berry", db.DB_KEYFIRST )
# cur.put( b'blue', "lemon", db.DB_KEYFIRST )
# cur.first()

# # prints no. of rows that have the same key as
# # the key of the row cursor is pointing to
# print( cur.count() )

# # prints the next key-data pair if it is a duplicate
# print( cur.next_dup() ) 

# curs.close()
# database.close()
