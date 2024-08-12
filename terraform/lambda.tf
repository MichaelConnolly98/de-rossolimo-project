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
    depends_on = [ aws_s3_object.lambda_extract ]
}