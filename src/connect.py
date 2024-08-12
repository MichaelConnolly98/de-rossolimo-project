from pg8000.native import Connection
def connect_to_db():
    return Connection(
        user="project_team_8",
        password="KE26RuOMaj5BlX7",
        database="totesys",
        host="nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com",
        port=5432
    )
# output = connect_to_db()
# query = output.run("SELECT * FROM design")
# print(query)