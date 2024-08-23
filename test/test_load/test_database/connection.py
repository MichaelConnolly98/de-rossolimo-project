# Create your connection to the DB in this file #
# Ensure that 'db' is a variable that can be accessed #

from pg8000.native import Connection
from pg8000.exceptions import DatabaseError
import os
from dotenv import load_dotenv

def test_local_db_connect():

    load_dotenv()
    user=os.getenv("PG_USER")
    password=os.getenv("PG_PASSWORD")
    database=os.getenv("PG_DATABASE")
    host=os.getenv("PG_HOST")
    port=int(os.getenv("PG_PORT"))

    # print("Username:", os.environ.get('DB_USERNAME'))
    # print("Database:", os.environ.get('DB_DATABASE'))
    # print("Password:", os.environ.get('DB_PASSWORD'))  # Don't print the actual password for security reasons
    # print("Port:", os.environ.get('DB_PORT_NO'))
    # return Connection(username, database=database)

    try:
        connection = Connection(user=user, password=password, database=database, port=port, host=host)
    except DatabaseError:
        connection = Connection(user=user, password=password, database='postgres', port=port, host=host)
        create_database_command = 'CREATE DATABASE test_totesys'
        connection.run(create_database_command)

    return connection

db = test_local_db_connect()