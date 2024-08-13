data "aws_iam_policy_document" "cw_document" {
  statement {
    effect = "Allow"
        
    actions = [ "logs:CreateLogGroup"]

    resources =  ["arn:aws:logs:eu-west-2:${data.aws_caller_identity.current.account_id}:*"]
  }
  statement {
     effect = "Allow"
      actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]
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
