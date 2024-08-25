from test_warehouse_data.seed_m import seed
from test_warehouse_data.connection_m import local_db_connect

# Do not change this code
def run_seed():
    try:
        seed()
    except Exception as e:
        print(e)
