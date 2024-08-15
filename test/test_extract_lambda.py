import os

with open("s3_bucket_name.txt") as file:
    os.environ["S3_BUCKET_NAME"] = file.readline()
