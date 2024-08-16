#bucket which stores data extracted from totesys warehouse
resource "aws_s3_bucket" "data_bucket" {
  bucket_prefix = "${var.ingestion_bucket_prefix}-"
  tags = {
    BucketType = "Data"
    BucketFunction = "IngestionContainer"
  }
}

#resource to apply versioning to data bucket above
resource "aws_s3_bucket_versioning" "data_bucket" {
  bucket = aws_s3_bucket.data_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

#resource to add rule to bucket that keeps data immutable
resource "aws_s3_bucket_object_lock_configuration" "data_bucket" {
  depends_on = [ aws_s3_bucket_versioning.data_bucket ]
  bucket = aws_s3_bucket.data_bucket.id

  rule {
    default_retention {
      mode = "GOVERNANCE"
      days = 3626
    }
  }
}

#s3 bucket that contains lambda code 
resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "${var.code_bucket_prefix}-"
  tags = {
    BucketType = "Code"
    BucketFunction = "LambdaContainer"
  }
}

#upload extractlambda archive file to lambda code bucket
resource "aws_s3_object" "lambda_extract" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key = "lambda/extract.zip"
  source = "${path.module}/../lambda_packages/extract.zip"
  etag = filemd5("${path.module}/../lambda_packages/extract.zip")
  depends_on = [ data.archive_file.lambda_extract ]
}

#upload extract lambda layer code to lambda code bucket
resource "aws_s3_object" "layer_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key = "lambda/layer.zip"
  # etag = filemd5("${path.module}/../layer.zip")
  source = "${path.module}/../layer.zip"
}

##########################
#bucket for processed data
##########################
resource "aws_s3_bucket" "processed_data_bucket" {
  bucket_prefix = "${var.code_bucket_prefix}-"
  tags = {
    BucketType = "ProcessedData"
    BucketFunction = "HoldsProcessedData"
    }
}



