# Import statements
import psycopg2
import sys
import psycopg2.extras
from csv import DictReader
from config import *

# Write code / functions to set up database connection and cursor here.
try:
    if db_password != "":
        conn = psycopg2.connect("dbname='{0}' user='{1}' password='{2}'".format(db_name, db_user, db_password))
        print("Success connecting to database")
    else:
        conn = psycopg2.connect("dbname='{0}' user='{1}'".format(db_name, db_user))
        print("Success connecting to database")
except:
    print("Unable to connect to the database. Check server and credentials.")
    sys.exit(1) # Stop running program if there's no db connection.

cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) # So you can insert by column name, instead of position, which makes the Python code even easier to write!


## Code to DROP TABLES IF EXIST IN DATABASE (so no repeats)
cur.execute("DROP TABLE IF EXISTS Sites")
cur.execute("DROP TABLE IF EXISTS States")

cur.execute("""CREATE TABLE IF NOT EXISTS States(ID SERIAL PRIMARY KEY,
                                                 Name VARCHAR(40) UNIQUE)""")


cur.execute("""CREATE TABLE IF NOT EXISTS Sites(ID SERIAL PRIMARY KEY,
                                                Name VARCHAR(128) UNIQUE,
                                                Type VARCHAR(128),
                                                State_ID INTEGER REFERENCES States(ID),
                                                Location VARCHAR(255),
                                                Description TEXT)""")


def insert_into_states(state):
    sql = """INSERT INTO States(Name) VALUES(%s)"""
    cur.execute(sql,(state[0:40],))
    cur.execute("SELECT MAX(ID) from States")
    state_id = cur.fetchone()['max']
    return state_id


def insert_into_Sites(filename, conn=conn, cur=cur):
    state_reader = DictReader(open(filename, 'r'))
    state_name = filename.split('.')[0]
    state_id = insert_into_states(state_name)
    sql = """INSERT INTO Sites(Name, Type, State_ID, Location, Description) 
             VALUES(%s, %s, %s, %s, %s)"""
    for line_dict in state_reader:
        name_val = line_dict['NAME']
        type_val = line_dict['TYPE']
        location = line_dict["LOCATION"]
        description = line_dict["DESCRIPTION"].strip()
        cur.execute(sql,(name_val[0:128], type_val[0:128], state_id, location[0:255], description,))
    conn.commit()


insert_into_Sites('Arkansas.csv')
insert_into_Sites('Michigan.csv')
insert_into_Sites('California.csv')




# Write code to make queries and save data in variables here.

cur.execute("""SELECT Location FROM Sites """)
all_locations = cur.fetchall()
# print(all_locations)
# print("\n")
cur.execute("""SELECT Name FROM Sites
               WHERE Description LIKE '%beautiful%' """)
beautiful_sites = cur.fetchall()
# print(beautiful_sites)
# print("\n")

cur.execute("""SELECT COUNT(ID) FROM Sites
               WHERE Type = 'National Lakeshore' """)
natl_lakeshores = cur.fetchall()
# print(natl_lakeshores)
# print("\n")

cur.execute("""SELECT Sites.Name
               FROM Sites INNER JOIN States
               ON States.ID = Sites.State_ID
               WHERE States.Name = 'Michigan' """)
michigan_names = cur.fetchall()
# print(michigan_names)
# print("\n")

cur.execute(""" SELECT Count(Sites.State_ID)
                FROM Sites INNER JOIN States
                ON States.ID = Sites.State_ID
                WHERE States.Name = 'Arkansas' """)
total_number_arkansas = cur.fetchall()
# print(total_number_arkansas)
print("finished all queries")
