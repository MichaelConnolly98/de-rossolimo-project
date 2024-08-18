from utils.extract_data import extract_func
import boto3
import json
import pytest

with open("s3_bucket_name.txt", "r", encoding="utf-8") as f:
    S3_BUCKET_NAME = f.readline()

@pytest.mark.it("""Checks all tables id columns in totesys database against all
                id values in s3 bucket tables, asserting equality """)
def test_all_file_ids_match():

    #all data in sql database
    database_data = extract_func()
    table_names = [x for x in database_data["all_data"].keys()]

    #loops through for each table in table_names
    for table in table_names:
        
        #gets id column data 
        id_num = []
        for ele in database_data["all_data"][table]:
            id_num.append(ele[f"{table}_id"])

        #get keys for all files in table={table}/ filepath
        s3_client = boto3.client("s3")
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=f"table={table}/")
        #slash in prefix is important, otherwise payments and payment types
        #will both come through when checking for payments
        key_list = [el["Key"] for el in response["Contents"]]


        # gets file contents of each Key returned above, saving as python dict
        data = []
        for k in key_list:
            f = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=k)
            a = f["Body"].read().decode("utf-8")
            dict_content = json.loads(a)
            data.append(dict_content)


        # gets id data from each file with contents from file contents above
        id_s3_bucket = []
        for element in data:
            if element[table]:
                for el in element[table]:
                    id_s3_bucket.append(el[f"{table}_id"])

        # asserts data in table is same as data in bucket
        assert len(id_s3_bucket) > 0
        assert len(id_num) > 0
        assert id_s3_bucket== id_num

