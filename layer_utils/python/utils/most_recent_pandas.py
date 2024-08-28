import boto3
from utils.extract_data import get_connection
from pg8000.exceptions import DatabaseError
import logging
import json
from pprint import pprint
import pandas as pd
from botocore.exceptions import ClientError
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if os.getenv("S3_DATA_BUCKET_NAME") is not None:
    S3BUCKETDATA = os.environ["S3_DATA_BUCKET_NAME"]
else:
    S3BUCKETDATA = "de-rossolimo-ingestion-20240812125359611100000001"


# should i put the get_connection in the parameters instead?
def get_table_names_single(connection=get_connection):
    """
    Returns list of table names in database

    Parameters:
    pg8000 connection function which returns a Connection object

    Returns:
    List of table names for given database
    """
    conn = None
    try:
        conn = connection()
        result = conn.run("""SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'public'""")
        result_no_prisma = [
            x[0] for x in result if x[0] != '_prisma_migrations']

        return result_no_prisma

    except DatabaseError as e:
        logging.error(
            {"Result": "Failure",
             "Error": f"A database error has occured: {str(e)}"}
        )
        raise DatabaseError("A database connection error has occured")
    except Exception as err:
        logging.error(
            {
                "Result": "Failure",
                "Error": f"A database connection exception has occured:" +
                f"{str(err)}",
            }
        )
        raise Exception("An exception has occured")
    finally:
        conn.close()

# change bucket name to dynamic


def get_most_recent_key_per_table_from_s3_single(
        s3_table_name_prefix, bucket_name=S3BUCKETDATA):
    """
    Gets all key paths for a file prefix in s3 bucket

    Parameters:
    s3_table_name_prefix - string of a prefix for a filepath in s3
    bucket_name - string of a bucket name

    Returns:
    List of keys for a particular file prefix
    """
    get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))

    try:
        s3_client = boto3.client("s3")
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=f"table={s3_table_name_prefix}/")["Contents"]
        last_added = [obj['Key'] for obj in sorted(
            response, key=get_last_modified)][-1]
        return last_added

    except ClientError as e:
        logging.error(
            {"Result": "Failure",
             "Error": f"A Client Error error has occured: {str(e)}"}
        )
        raise e
    except KeyError as k:
        if "Contents" in str(k):
            logging.error({"Result": "Failure",
                           "Error": f"No contents in Prefix or Bucket, " +
                           f"{str(k)}"})
        raise k


# change bucket name to dynamic
def get_s3_file_content_from_key_single(key: list, bucket_name=S3BUCKETDATA):
    """
    Gets file contents from s3 key list

    Parameters:
    key_list - a list of file paths for an s3 bucket

    Returns:
    List of Python dictionary of data contained in each key path
    If key_list is empty list, returns empty list
    """

    try:
        s3_client = boto3.client("s3")
        data = []

        f = s3_client.get_object(Bucket=bucket_name, Key=key)
        a = f["Body"].read().decode("utf-8")
        dict_content = json.loads(a)
        for k, v in dict_content.items():
            if v != []:
                data.append(dict_content)

    except ClientError as e:
        logging.error(
            {"Result": "Failure",
             "Error": f"A Client Error error has occured: {str(e)}"}
        )
        raise e
    # except Exception as exception:
    #     logging.error({"Result": "Failure",
    # "Error": f"An exception has occured: {str(exception)}"})
    #     raise Exception("An error has occured")
    return data


def file_data_single():
    """
    Gets file data for every table stored in s3 bucket

    Returns:
    Python dict of form
    {table_name: [{dict for each row},{}], table_name2: [{},{},{}], etc...}
    """
    # try:
    table_names = get_table_names_single()

    # {'address': [], 'staff': [], etc...}
    file_contents_dict = {table_name: [] for table_name in table_names}

    for table in table_names:
        key = get_most_recent_key_per_table_from_s3_single(table)
        file_contents = get_s3_file_content_from_key_single(key)
        if file_contents:
            # take first item of list, which has keys already in
            list_to_add_to = file_contents[0]
            if len(file_contents) == 1:
                file_contents_dict[table] = list_to_add_to[table]
            else:
                # appends other dictionaries in other
                # elements to first list element
                for el in file_contents[1:]:
                    for ele in el[table]:
                        # list to add format_to final form:
                        # {address: [{},{},{}]}
                        list_to_add_to[table].append(ele)
                        file_contents_dict[table] = list_to_add_to[table]

    return file_contents_dict
    # except Exception as exception:
    #     logging.error({"Result": "Failure",
    # "Error": f"An exception has occured: {str(exception)}"})
    #     raise Exception("An error has occured")


def dataframe_creator_single(table_name, file_dict=None):
    """
    Converts dict to pandas dataframe

    Parameters:
    table_name - gets specific table name from file_data provided
    creates dataframe from each one, else uses the table parameter provided

    Returns:

    a single dataframe objects
    """

    try:
        if file_dict[table_name]:
            df = pd.json_normalize(file_dict[table_name])
            df.name = table_name
            df.set_index(f"{table_name}_id", inplace=True, drop=True)
            df.sort_index(inplace=True)
            return df
        else:
            return None

    except Exception as exception:
        logging.error({
            "Result": "Failure",
            "Error": f"An exception has occured: {str(exception)}"}
        )
        raise exception
