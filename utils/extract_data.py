from pg8000.native import Connection, literal, identifier, DatabaseError, InterfaceError
import boto3
from botocore.exceptions import ClientError
import json
import logging
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


def get_db_credentials(secret_name="totesys", sm_client=boto3.client("secretsmanager")):
    """
    Connects to AWS secrets manager and retrieves secret

    Parameters:
    secret_name - default "totesys". Name of secret to retrieve
    sm_client - an instance of a secrets manager boto3 client

    Returns:
    Dictionary containing secret key:pairs
    """

    try:
        get_secret_value_response = sm_client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logging.error(
            {"Result": "Failure", "Error": f"A secrets manager error has occured: {str(e)}"}
        )
        raise e

    secret = json.loads(get_secret_value_response["SecretString"])

    return secret


def get_connection():
    """
    Connects to PSQL database, using credentials from get_db_credentials
    function

    Returns:
    Instance of pg8000.native Connection object
    """
    try:
        credentials_dict = get_db_credentials()

        return Connection(
            user=credentials_dict["username"],
            password=credentials_dict["password"],
            database=credentials_dict["dbname"],
            host=credentials_dict["host"],
            port=credentials_dict["port"],
        )
    except DatabaseError as e:
        logging.error(
            {"Result": "Failure", "Error": f"A database error has occured: {str(e)}"}
        )
        raise DatabaseError("A database connection error has occured")
    except InterfaceError as interr:
        logging.error(
            {
                "Result": "Failure",
                "Error": f"A database connection error has occured: {str(interr)}",
            }
        )
        raise InterfaceError("A database connection error has occured")
    except Exception as err:
        logging.error(
            {
                "Result": "Failure",
                "Error": f"A database connection exception has occured: {str(err)}",
            }
        )
        raise Exception("A database connection exception has occured")


def extract_func(datetime="2000-01-01 00:00"):
    """
    Interacts with PSQL database, selecting all data updated from a time
    given as a parameter

    Parameters:
    datetime - The time query used in SQL query, for which data will be
    returned if updated after given datetime

    Returns:
    Dictionary of format {'all_data': {'table_1: data}, {'table_2: data}, ...}
    containing all data last updated after given time
    """
    try:
        with get_connection() as conn:
            table_names_sql_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public'
            AND table_type='BASE TABLE'
            """
            table_names_nested_list = conn.run(table_names_sql_query)
            table_names_flattened_list = [
                element[0]
                for element in table_names_nested_list
                if element[0] != "_prisma_migrations"
            ]

        data_dict = {}
        for table in table_names_flattened_list:
            data_dict[table] = query_table(table, datetime)

        all_data_dict = {"all_data": data_dict}
        logger.info({"Result": "Success", "Message": "extract function ran successfully"})
        return all_data_dict

    except DatabaseError as e:
        logging.error({"Result": "Failure", "Error": f"A database error has occured: {str(e)}"})
        raise DatabaseError("A database error has occured")

    except Exception as exception:
        logging.error({"Result": "Failure", "Error": f"An exception has occured: {str(exception)}"})
        raise Exception("An error has occured")


def query_table(table_name, datetime):
    """
    Queries database for individual table names and returns data
    to extract function

    Parameters:
    table_name - name of the table, fed in by extract function
    datetime - data to restrict search by, fed in by extract function

    Returns:
    List of dictionaries, each element representing one row in
    """
    try:
        with get_connection() as conn:
            table_query = f"""SELECT * FROM {identifier(table_name)}
                                WHERE last_updated > {literal(datetime)};"""
            data = conn.run(table_query)
            columns = [column["name"] for column in conn.columns]

            results_list = []
            for row in data:
                result = dict(zip(columns, row))
                results_list.append(result)
            return results_list
    except DatabaseError as e:
        logging.error({"Result": "Failure", "Error": f"A database query error has occured: {str(e)}"})
        raise DatabaseError("A database query error has occured")

    except Exception as exception:
        logging.error({"Result": "Failure", "Error": f"An exception has occured: {str(exception)}"})
        raise Exception("A query exception has occured")
