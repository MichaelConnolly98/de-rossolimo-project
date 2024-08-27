import pg8000
from pg8000.exceptions import DatabaseError
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def non_local_db_connect(secret):
    try:
        conn = pg8000.connect(
        user=secret["username"],
        password=secret["password"],
        database=secret["dbname"],
        host=secret["host"],
        port=secret["port"]
        )
        return conn
    
    except DatabaseError as d:
        logging.error({"Result": "Failure", "error": f"Database Error occured: {d}"})
        raise d
