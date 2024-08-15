#####################
#lambda to s3 policy documents and roles
#####################

resource "aws_iam_role" "lambda_role" {
  name_prefix        = "role-de-rossolimo-lambdas-"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

data "aws_iam_policy_document" "s3_document" {
  statement {

    actions = [ "s3:PutObject", "s3:GetObject" ]

    resources = [
      "${aws_s3_bucket.data_bucket.arn}/*",
      "${aws_s3_bucket.code_bucket.arn}/*",
    ]
  }
}


resource "aws_iam_policy" "s3_policy" {
  name_prefix = "s3-policy-ingestion-lambda-"
  policy      = data.aws_iam_policy_document.s3_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}


#####################
#cloudwatch policy and role (and attachment to lambda)
#####################


data "aws_iam_policy_document" "cw_document" {
  statement {
    effect = "Allow"
        
    actions = [ "logs:CreateLogGroup"]

    resources =  ["arn:aws:logs:eu-west-2:${data.aws_caller_identity.current.account_id}:*"]
  }
  statement {
     effect = "Allow"
      actions = [ "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams" ]
      resources = ["arn:aws:logs:eu-west-2:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/extract-de_rossolimo:*"]
  }
}


# Create
resource "aws_iam_policy" "cw_policy" {
  name_prefix = "cw-policy-extract-de_rossolimo-"
  policy = data.aws_iam_policy_document.cw_document.json
}

# Attach
resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
  role = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
}

#allows lambda to access secrets
resource "aws_iam_role_policy" "sm_policy" {
  name = "sm_access_permissions"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "secretsmanager:GetSecretValue",
        ]
        Effect   = "Allow"
        Resource = "*"
      },
    ]
  })
}
