
resource "aws_cloudwatch_event_rule" "scheduler" {
  name = "trigger-step-function"
  description = "trigger lambda every 20 minutes"
  schedule_expression = "rate(20 minutes)"
}

resource "aws_cloudwatch_event_target" "step_target" {
  rule      = aws_cloudwatch_event_rule.scheduler.name
  arn       = aws_sfn_state_machine.step_function_ETL.arn
  role_arn  = aws_iam_role.step_function.arn
}

resource "aws_iam_role" "step_function" {
  name_prefix        = "role-cloudwatch-stepfunction-"
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
                        "states.amazonaws.com",
                        "events.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

data "aws_iam_policy_document" "cloudwatch_start_statemachine" {
  statement {

    actions = [ "states:startExecution" ]
    resources = [aws_sfn_state_machine.step_function_ETL.arn]
  }
}


resource "aws_iam_policy" "step_function_policy" {
  name_prefix = "step-function-policy-cloudwatch-"
  policy      = data.aws_iam_policy_document.cloudwatch_start_statemachine.json
}

resource "aws_iam_role_policy_attachment" "cloudwatch_step_policy_attachment" {
  role       = aws_iam_role.step_function.name
  policy_arn = aws_iam_policy.step_function_policy.arn
}