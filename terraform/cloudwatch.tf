########################
#extract lambda cloudwatch
########################

resource "aws_cloudwatch_log_metric_filter" "error" {
  name = "ERROR"
  pattern = "ERROR"
  log_group_name = "/aws/lambda/extract-de_rossolimo"


  metric_transformation {
    name = "ErrorOccur"
    namespace = "LambdaExtractRossolimoNameSpace"
    value = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "error_alarm" {
  alarm_name = "ErrorAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold = 0
  evaluation_periods = 1
  namespace = "LambdaExtractRossolimoNameSpace"
  metric_name = aws_cloudwatch_log_metric_filter.error.id
  alarm_actions = [aws_sns_topic.error_sns_topic.arn]
  period = 60
  statistic = "Sum"
}

resource "aws_cloudwatch_metric_alarm" "duration_alarm" {
  alarm_name = "DurationAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold = 40
  evaluation_periods = 1
  namespace = "AWS/Lambda"
  metric_name = "Duration"
  alarm_actions = [aws_sns_topic.duration_sns_topic.arn]
  period = 60
  statistic = "Maximum"
}

resource "aws_sns_topic" "error_sns_topic" {
    name = "warn-error-occured"
}

resource "aws_sns_topic_subscription" "email_error" {
  for_each = toset("${var.emails}")
  topic_arn = aws_sns_topic.error_sns_topic.arn
  protocol  = "email"
  endpoint = each.value
}

resource "aws_sns_topic" "duration_sns_topic" {
    name = "warn-40s-duration-exceeded"
}

resource "aws_sns_topic_subscription" "duration_exceeded" {
  for_each = toset("${var.emails}")
  topic_arn = aws_sns_topic.duration_sns_topic.arn
  protocol  = "email"
  endpoint = each.value
}

#########################
# transform lambda cloudwatch
#########################

#########################
#attempt to make alarm trigger off any error in any lambda
#########################
resource aws_cloudwatch_metric_alarm "all_lambdas_errors_alarm" {
  alarm_name          = "all-lambdas-errors"
  alarm_description   = "Lambdas with errors"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  threshold           = 0
  period              = 60
  unit                = "Count"

  namespace   = "AWS/Lambda"
  metric_name = "Errors"
  statistic   = "Maximum"

  alarm_actions = [aws_sns_topic.duration_sns_topic.arn, aws_sns_topic.error_sns_topic.arn]
}