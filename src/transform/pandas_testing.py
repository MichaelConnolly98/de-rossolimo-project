import boto3
from utils.extract_data import get_connection
from pg8000.exceptions import DatabaseError
import logging
import json
from pprint import pprint
import pandas as pd
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#should i put the get_connection in the parameters instead? 
def get_table_names(connection=get_connection):
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
        result_no_prisma =  [x[0] for x in result if x[0] != '_prisma_migrations']
        
        return result_no_prisma
        
    except DatabaseError as e:
        logging.error(
            {"Result": "Failure", "Error": f"A database error has occured: {str(e)}"}
        )
        raise DatabaseError("A database connection error has occured")
    except Exception as err:
        logging.error(
            {
                "Result": "Failure",
                "Error": f"A database connection exception has occured: {str(err)}",
            }
        )
        raise Exception("An exception has occured")
    finally:
        conn.close()

#change bucket name to dynamic
def get_keys_from_s3(s3_table_name_prefix, bucket_name="de-rossolimo-ingestion-20240812125359611100000001"):
    """
    Gets all key paths for a file prefix in s3 bucket

    Parameters:
    s3_table_name_prefix - string of a prefix for a filepath in s3
    bucket_name - string of a bucket name

    Returns:
    List of keys for a particular file prefix
    """
    try:
        s3_client = boto3.client("s3")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"table={s3_table_name_prefix}/")

        key_list = [el["Key"] for el in response["Contents"]]
        return key_list
    
    except ClientError as e:
        logging.error(
            {"Result": "Failure", "Error": f"A Client Error error has occured: {str(e)}"}
        )
        raise e
    except KeyError as k:
        if "Contents" in str(k):
            logging.error({"Result": "Failure", "Error": f"No contents in Prefix or Bucket, {str(k)}"})
        raise k

    except Exception as exception:
        logging.error({"Result": "Failure", "Error": f"An exception has occured: {str(exception)}"})
        raise Exception("An error has occured")

#change bucket name to dynamic
def get_s3_file_content_from_keys(key_list: list, bucket_name="de-rossolimo-ingestion-20240812125359611100000001"):
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
        for k in key_list:
            f = s3_client.get_object(Bucket=bucket_name, Key=k)
            a = f["Body"].read().decode("utf-8")
            dict_content = json.loads(a)
            for k, v in dict_content.items():
                if v != []:
                    data.append(dict_content)

    except ClientError as e:
        logging.error(
            {"Result": "Failure", "Error": f"A Client Error error has occured: {str(e)}"}
        )
        raise e
    except Exception as exception:
        logging.error({"Result": "Failure", "Error": f"An exception has occured: {str(exception)}"})
        raise Exception("An error has occured")
    return data

def file_data():
    """
    Gets file data for every table stored in s3 bucket

    Returns:
    Python dict of form 
    {table_name: [{dict for each row},{}], table_name2: [{},{},{}], etc...}
    """
    try:
        table_names = get_table_names()

        #{'address': [], 'staff': [], etc...}
        file_contents_dict = {table_name: [] for table_name in table_names}

        for table in table_names:
            key_list = get_keys_from_s3(table)
            file_contents = get_s3_file_content_from_keys(key_list)

            #take first item of list, which has keys already in
            list_to_add_to = file_contents[0]
            if len(file_contents) == 1:
                file_contents_dict[table] = list_to_add_to[table]
            else:
                #appends other dictionaries in other elements to first list element
                for el in file_contents[1:]:
                    for ele in el[table]:
                    #list to add format_to final form: {address: [{},{},{}]}
                        list_to_add_to[table].append(ele)
                        file_contents_dict[table] = list_to_add_to[table]

        #dont need to save it to a file, just means I don't have to run it everytime
        with open("./pandas_test_data.json", "w", encoding="utf-8") as f:
            json.dump(file_contents_dict, f)
        return file_contents_dict
    except Exception as exception:
        logging.error({"Result": "Failure", "Error": f"An exception has occured: {str(exception)}"})
        raise Exception("An error has occured")

def dataframe_creator(table_name=None):
    """
    Converts json data to pandas dataframe

    Parameters:
    table_name - default None. If None, get's all table names and 
    creates dataframe from each one, else uses the table parameter provided

    Returns:
    list of dataframe if no table_name provided
    OR
    a single dataframe object
    """

    #currently opening from test_data - fine for first big dump, 
    #need to sort out how this will pull from buckets in future
    #file_data returns dicts always even if no data
    #so it may be OK to make into a dataframe anyway
    try:
        with open ("./pandas_test_data.json", "r") as f:
            file_dict = json.load(f)
            dataframe_list = []
            if not table_name:
                tables = get_table_names()
                for table in tables:
                    df = pd.json_normalize(file_dict[table])
                    df.name = table
                    df.set_index(f"{table}_id", inplace=True, drop=True)
                    df.sort_index(inplace=True)
                    dataframe_list.append(df)
                return dataframe_list
                
            else:
                df = pd.json_normalize(file_dict[table_name])
                df.name = table_name
                df.set_index(f"{table_name}_id", inplace=True, drop=True)
                df.sort_index(inplace=True)
                return df
            
    except Exception as exception:
        logging.error({
            "Result": "Failure",
            f"Error": "An exception has occured: {str(exception)"}
    )
        raise exception
            


    















