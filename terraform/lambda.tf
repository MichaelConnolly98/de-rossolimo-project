data "archive_file" "lambda_extract" {
    type = "zip"
    output_file_mode = "0666"
    source_file = "${path.module}/../src/extract.py"
    output_path = "${path.module}/../lambda_packages/extract.zip"
}

resource "aws_lambda_function" "lambda_extract" {
    function_name = "${var.extract_lambda}-de_rossolimo"
    source_code_hash = data.archive_file.lambda_extract.output_base64sha256
    s3_bucket = aws_s3_bucket.code_bucket.bucket
    s3_key = "lambda/extract.zip"
    role =  aws_iam_role.lambda_role.arn
    handler = "extract.lambda_handler"
    runtime = "python3.12"
    timeout = 10
    depends_on = [ aws_s3_object.lambda_extract, aws_lambda_layer_version.dependency_layer ]
    layers = [ aws_lambda_layer_version.dependency_layer.arn ]
    environment {
      variables = {
        S3_BUCKET_NAME = aws_s3_bucket.data_bucket.id
      }
    }
}

data "archive_file" "layer" {
  type = "zip"
  output_file_mode = "0666"
  source_dir =     "${path.module}/../layer"
  output_path      = "${path.module}/../layer.zip"
}

resource "aws_lambda_layer_version" "dependency_layer" {
  layer_name          = "dependency_layer"
  compatible_runtimes = [ "python3.12" ]
  s3_bucket           = aws_s3_bucket.code_bucket.bucket
  s3_key              = "lambda/layer.zip"
  depends_on = [ aws_s3_object.layer_code ]
  source_code_hash = data.archive_file.layer.output_base64sha256
}