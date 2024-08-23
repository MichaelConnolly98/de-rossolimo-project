data "aws_iam_policy_document" "state_machine_trust_policy" {
    statement {
        effect = "Allow"

        principals {
            type = "Service"
            identifiers = ["states.amazonaws.com"]
        }
        actions = [
            "sts:AssumeRole",
            ]

    }
}

resource "aws_iam_role" "step_function_role" {
    name_prefix = "ETL-step-function-role-"
    assume_role_policy = data.aws_iam_policy_document.state_machine_trust_policy.json

}

data "aws_iam_policy_document" "state_machine_policy" {
    statement {
            effect = "Allow"
            actions =  [
                "lambda:InvokeFunction"
            ]
            resources = [
                "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:extract-de_rossolimo:*",
            ]
        }
        statement {
            effect = "Allow"
            actions =   [
                "lambda:InvokeFunction"
            ]
            resources = [
                "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:extract-de_rossolimo",
            ]
        }
        statement {
            effect = "Allow"
            actions =  [
                "lambda:InvokeFunction"
            ]
            resources = [
                "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:transform-de_rossolimo:*",
            ]
        }
        statement {
            effect = "Allow"
            actions =   [
                "lambda:InvokeFunction"
            ]
            resources = [
                "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:transform-de_rossolimo",
            ]
        }
        
}

resource "aws_iam_role_policy" "state_machine_policy" {
    role = aws_iam_role.step_function_role.id
    policy = data.aws_iam_policy_document.state_machine_policy.json
}

resource "aws_sfn_state_machine" "step_function_ETL" {
    name = "step-function-ETL"
    role_arn = aws_iam_role.step_function_role.arn
    definition = templatefile("${path.module}/definition.json",
    {aws_region = data.aws_region.current.name, 
    aws_id = data.aws_caller_identity.current.account_id})
}