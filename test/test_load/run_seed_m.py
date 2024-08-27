from seed_m import seed
from utils.load_connection_m import local_db_connect

# Do not change this code
def run_seed():
    try:
        seed()
    except Exception as e:
        print(e)
