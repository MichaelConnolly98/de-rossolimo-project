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
        print(table)
        assert len(id_s3_bucket) > 0
        assert len(id_num) > 0

        #sort the set of unique id's in ascending order
        set_id_s3 = sorted(set(id_s3_bucket))
        set_id_num = sorted(set(id_num))
        #take all but last 100 values (as new data may not be in s3)
        set_id_s3_not_end = set_id_s3[:len(set_id_s3) - 100]
        set_id_num_not_end = set_id_num[:len(set_id_s3) - 100]
        
        assert set_id_s3_not_end == set_id_num_not_end

