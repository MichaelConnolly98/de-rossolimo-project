from utils.extract_data import extract_func
import boto3
from datetime import datetime
import json
import pytest

with open("s3_bucket_name.txt", "r", encoding="utf-8") as f:
    S3_BUCKET_NAME = f.readline()


@pytest.mark.it(
    """Checks design table data last_updated_data from OLTP database against 
    design table data uploaded to ingestion bucket, asserting equality"""
)
def test_design_table_data_matches_s3_ingestion_bucket_data():

    # pulls all data from extract function, and filters for design table,
    # then last_updated values (datetime format)
    last_updates = []
    full_data = extract_func()
    for el in full_data["all_data"]["design"]:
        last_updates.append(el["last_updated"])

    # gets all object keys for files in ingestion bucket design table path
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="table=design")
    key_list = [el["Key"] for el in response["Contents"]]

    # gets file contents of each Key returned above, saving as python dict
    data = []
    for k in key_list:
        f = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=k)
        a = f["Body"].read().decode("utf-8")
        dict_content = json.loads(a)
        data.append(dict_content)

    # gets last updated for each file with contents from file contents above
    last_updated_data = []
    for des in data:
        design_data = des["design"]
        if design_data:
            for el in design_data:
                last_updated_data.append(el["last_updated"])

    # converts string time format to datetime object
    final_updated_data = []
    for el in last_updated_data:
        if dat_obj := datetime.strptime(el, "%Y-%m-%d %H:%M:%S.%f"):
            #'2024-08-05 16:31:10.118000'
            final_updated_data.append(dat_obj)

    # asserts data in table is same as data in bucket
    assert len(final_updated_data) > 0
    assert len(last_updates) > 0
    assert final_updated_data == last_updates
