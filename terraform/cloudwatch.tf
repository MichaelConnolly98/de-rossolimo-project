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
  count = length(local.emails)
  topic_arn = aws_sns_topic.error_sns_topic.arn
  protocol  = "email"
  endpoint = local.emails[count.index]
}

resource "aws_sns_topic" "duration_sns_topic" {
    name = "warn-40s-duration-exceeded"
}

resource "aws_sns_topic_subscription" "duration_exceeded" {
  count = length(local.emails)
  topic_arn = aws_sns_topic.duration_sns_topic.arn
  protocol  = "email"
  endpoint = local.emails[count.index]
}
