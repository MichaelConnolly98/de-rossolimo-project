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