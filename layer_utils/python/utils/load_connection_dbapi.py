import pg8000
from dotenv import load_dotenv
import os


def connection():
    load_dotenv()
    conn = pg8000.connect(
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        database=os.getenv("PG_DATABASE"),
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT"))
    )
    return conn
