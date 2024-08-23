# Create your connection to the DB in this file #
# Ensure that 'db' is a variable that can be accessed #

from pg8000.native import Connection
import os

def test_local_db_connect():
    
    username = os.environ['DB_USERNAME']
    database = os.environ['DB_DATABASE']
    password = os.environ['DB_PASSWORD']
    port_no = os.environ['DB_PORT_NO']
    host = 'localhost'

    # print("Username:", os.environ.get('DB_USERNAME'))
    # print("Database:", os.environ.get('DB_DATABASE'))
    # print("Password:", os.environ.get('DB_PASSWORD'))  # Don't print the actual password for security reasons
    # print("Port:", os.environ.get('DB_PORT_NO'))
    # return Connection(username, database=database)
    return Connection(username, database=database, password=password, port=port_no, host=host)
