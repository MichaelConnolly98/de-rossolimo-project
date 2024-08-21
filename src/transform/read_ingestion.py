import boto3
import pandas as pd

def read_ingestion_function():
    s3 = boto3.client("s3")
    BUCKETNAME = 'testing-problem-bucket-de-rossolimo'
    tables = s3.list_objects(Bucket=BUCKETNAME, Prefix='table', Delimiter='/')['CommonPrefixes']
    table_names = [table['Prefix'].removeprefix('table=').removesuffix('/') for table in tables]
    # Would need to add logic here so only the most recent files are used
    keys = [s3.list_objects(Bucket=BUCKETNAME, Prefix=f'table={table}')['Contents'][0]['Key'] for table in table_names]

    dataframes_dict = {}
    for key in keys:
        table_name = key.removeprefix('table=').split('/', 1)[0]
        series_data = pd.read_json(s3.get_object(Bucket=BUCKETNAME, Key=key)['Body'])
        df = pd.DataFrame(series_data[table_name].tolist())
        df.set_index(df.columns[0], inplace=True)
        dataframes_dict[table_name] = df
    
    # print currency output to file
    # dataframes_dict['currency'].to_csv(r'/Users/nicholasslocombe/northcoders/project/de-rossolimo-project/output2.txt', mode='w')
    return dataframes_dict