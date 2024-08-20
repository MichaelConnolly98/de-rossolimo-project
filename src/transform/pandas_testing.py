import boto3
from utils.extract_data import get_connection
from pg8000.exceptions import DatabaseError
import logging
import json
from pprint import pprint
import pandas as pd
import iso4217parse

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#should i put the get_connection in the parameters instead? 
def get_table_names():
    """
    Returns list of table names in totesys database
    """
    conn = None
    try:
        conn = get_connection()
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
        raise Exception("A database connection exception has occured")
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
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"table={s3_table_name_prefix}/")
    key_list = [el["Key"] for el in response["Contents"]]
    return key_list

#change bucket name to dynamic
def get_s3_file_content_from_keys(key_list: list, bucket_name="de-rossolimo-ingestion-20240812125359611100000001"):
    """
    Gets file contents from s3 key list

    Parameters:
    key_list - a list of file paths for an s3 bucket

    Returns:
    List of Python dictionary of data contained in each key path
    """
    s3_client = boto3.client("s3")
    data = []
    for k in key_list:
        f = s3_client.get_object(Bucket=bucket_name, Key=k)
        a = f["Body"].read().decode("utf-8")
        dict_content = json.loads(a)
        for k, v in dict_content.items():
            if v != []:
                data.append(dict_content)
        # if dict_content.values() != []:
        #     data.append(dict_content)
    return data

def file_data():
    """
    Gets file data for every table stored in s3 bucket

    Returns:
    Python dict of form 
    {table_name: [{dict for each row},{}], table_name2: [{},{},{}], etc...}
    """

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
    with open ("./pandas_test_data.json", "r") as f:
        file_dict = json.load(f)
        dataframe_list = []
        if not table_name:
            tables = get_table_names()
            for table in tables:
                df = pd.json_normalize(file_dict[table])
                #name the list so you can call the individual dataframes
                df.name = table
                dataframe_list.append(df)
            return dataframe_list
            
        else:
            df = pd.json_normalize(file_dict[table_name])
            #should probably sort by id_column. maybe have to do that before
            #creating the dataframe so it aligns with index column
            #maybe order by id_column and make that the index then delete
            return df
            
#just dim tables
# def dim_tables():
#     dataframe_list = dataframe_creator()
#     print(get_table_names())
#     for el in dataframe_list:
#         if el.name in ["address", "staff", "department", "counterparty", "design", "transaction", "design", "currency", "payment_type"]:
#             pass
#         else:
#             print("Not in list", el.name)


#might be that you want to be more specific - use drop method to remove certain columns by name 
#not by index reference
#use the id column from the table it's drawing from 
#need to change the indexes


def staff_dim():
    part_staff_df = dataframe_creator(table_name="staff")
    department_df = dataframe_creator(table_name="department")
    full_staff_df = pd.merge(part_staff_df, department_df[["department_id", "department_name", "location"]], on="department_id", how="left")
    #reorder columns
    cols = full_staff_df.columns.tolist()
    cols = cols[0:3] + cols[7:] + cols[4:5]
    full_staff_df = full_staff_df[cols]
    return full_staff_df

print(staff_dim())

def payment_dim():
    payment_df = dataframe_creator(table_name="payment_type")
    payment_df = payment_df.iloc[:,0:2]
    return payment_df

def counterparty_dim():
    part_counterparty_df = dataframe_creator(table_name="counterparty")
    address_df = dataframe_creator(table_name="address")
    full_counterparty_df = pd.merge(part_counterparty_df, address_df, left_on="legal_address_id",right_on="address_id", how="left")
    cols = full_counterparty_df.columns.tolist()
    cols = cols[0:2] + cols[8:15]
    full_counterparty_df = full_counterparty_df[cols]
    #fair few nones for address lines here, probably fine
    return counterparty_dim

def currency_name_maker(currency):
    """
    Takes a currency code and converts to a currency name
    """
    return iso4217parse.parse(currency)[0].name

def currency_dim():
    currency_df = dataframe_creator(table_name="currency")
    currency_df = currency_df.iloc[:, 0:2]
    currency_df["currency_name"] = currency_df["currency_code"].apply(currency_name_maker)
    return currency_df


    















