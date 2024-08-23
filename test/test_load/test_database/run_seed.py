from test.test_load.test_database.seed import seed
from test.test_load.test_database.connection import db

# Do not change this code
try:
    seed()
except Exception as e:
    print(e)
finally:
    db.close()
