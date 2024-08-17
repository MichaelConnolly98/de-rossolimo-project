# extract lambda directory archive file
data "archive_file" "lambda_extract" {
    type = "zip"
    output_file_mode = "0666"
    source_dir = "${path.module}/../src/extract"
    output_path = "${path.module}/../lambda_packages/extract.zip"
}

#extract lambda function 
resource "aws_lambda_function" "lambda_extract" {
    function_name = "${var.extract_lambda}-de_rossolimo"
    source_code_hash = data.archive_file.lambda_extract.output_base64sha256
    s3_bucket = aws_s3_bucket.code_bucket.bucket
    s3_key = "lambda/extract.zip"
    role =  aws_iam_role.lambda_role.arn
    handler = "extract_lambda.lambda_handler"
    runtime = "python3.12"
    timeout = 60
    depends_on = [ aws_s3_object.lambda_extract, aws_lambda_layer_version.dependency_layer, aws_lambda_layer_version.utility_layer ]
    layers = [ aws_lambda_layer_version.dependency_layer.arn, aws_lambda_layer_version.utility_layer.arn ]
    environment {
      variables = {
        S3_BUCKET_NAME = aws_s3_bucket.data_bucket.id
      }
    }
}

#extract layer archive file
data "archive_file" "layer" {
  type = "zip"
  output_file_mode = "0666"
  source_dir =     "${path.module}/../layer"
  output_path      = "${path.module}/../layer.zip"
}

#extract layer resource
resource "aws_lambda_layer_version" "dependency_layer" {
  layer_name          = "dependency_layer"
  compatible_runtimes = [ "python3.12" ]
  s3_bucket           = aws_s3_bucket.code_bucket.bucket
  s3_key              = "lambda/layer.zip"
  depends_on = [ aws_s3_object.layer_code ]
  source_code_hash = data.archive_file.layer.output_base64sha256
}
####################################
#utility layer
####################################
data "archive_file" "util_layer" {
  type = "zip"
  output_file_mode = "0666"
  source_dir =     "${path.module}/../layer_utils"
  output_path      = "${path.module}/../lambda_packages/utils.zip"
}

resource "aws_lambda_layer_version" "utility_layer" {
  layer_name          = "utility_layer"
  compatible_runtimes = [ "python3.12" ]
  s3_bucket           = aws_s3_bucket.code_bucket.bucket
  s3_key              = "lambda/utils.zip"
  depends_on = [ aws_s3_object.util_layer_code ]
  source_code_hash = data.archive_file.util_layer.output_base64sha256
}


####################################
#Transform Lambda Resources below
####################################
data "archive_file" "lambda_transform_data" {
  type = "zip"
  output_file_mode = "0666"
  source_dir = "${path.module}/../src/transform"
  output_path = "${path.module}/../lambda_packages/transform.zip"
}

resource "aws_lambda_function" "lambda_transform" {
     function_name = "${var.transform_lambda}-de_rossolimo"
     source_code_hash = data.archive_file.lambda_transform_data.output_base64sha256
     s3_bucket = aws_s3_bucket.code_bucket.bucket
     s3_key = "lambda/transform.zip"
     role =  aws_iam_role.transform_lambda_role.arn
     handler = "transform_lambda.lambda_handler"
     runtime = "python3.12"
     timeout = 60
     depends_on = [ aws_s3_object.lambda_transform, aws_lambda_layer_version.dependency_layer ]
     layers = [ aws_lambda_layer_version.dependency_layer.arn ]
    environment {
      variables = {
        S3_DATA_BUCKET_NAME = aws_s3_bucket.data_bucket.id
        S3_PROCESS_BUCKET_NAME = aws_s3_bucket.processed_data_bucket.id
      }
    }
}
