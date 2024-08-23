import pandas as pd
import pyarrow as pq
import boto3

'''function that reads from s3 bucket transforms parq back into dataframes'''

def load_pq_to_df(pq_file):
    s3 = boto3.client("s3")
    