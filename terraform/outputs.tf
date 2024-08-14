resource "local_file" "s3_bucket_name" {
    content = aws_s3_bucket.data_bucket.bucket
    filename = "${path.module}/../s3_bucket_name.txt"
}