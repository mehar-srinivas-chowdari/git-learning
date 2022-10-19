'''
import psycopg2

conn = psycopg2.connect(host='localhost',port=5432,database='postgres',user='postgres',password=1)
'''

#returns zero if the connection is active or returns a non zero value if the connection is closed or broken

def check_connection(conn):
    return conn.closed

print(check_connection(conn))
