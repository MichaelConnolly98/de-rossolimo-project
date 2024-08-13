resource "aws_s3_bucket" "data_bucket" {
  bucket_prefix = "${var.ingestion_bucket_prefix}-"
  tags = {
    BucketType = "Data"
    BucketFunction = "IngestionContainer"
  }
}


resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "${var.code_bucket_prefix}-"
  tags = {
    BucketType = "Code"
    BucketFunction = "LambdaContainer"
  }
}



resource "aws_s3_object" "lambda_extract" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key = "lambda/extract.zip"
  source = "${path.module}/../lambda_packages/extract.zip"
  etag = filemd5("${path.module}/../lambda_packages/extract.zip")
  depends_on = [ data.archive_file.lambda_extract ]
}