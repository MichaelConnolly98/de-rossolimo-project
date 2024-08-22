resource "local_file" "s3_bucket_name" {
    content = aws_s3_bucket.data_bucket.bucket
    filename = "${path.module}/../s3_bucket_name.txt"
}

resource "local_file" "process_s3_bucket_name" {
    content = aws_s3_bucket.processed_data_bucket.bucket
    filename = "${path.module}/../s3_process_bucket_name.txt"
}