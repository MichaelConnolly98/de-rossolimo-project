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

resource "aws_cloudwatch_metric_alarm" "great_quote_alarm" {
  alarm_name = "ErrorAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold = 1
  evaluation_periods = 1
  namespace = "LambdaExtractRossolimoNameSpace"
  metric_name = aws_cloudwatch_log_metric_filter.error.id
  alarm_actions = [aws_sns_topic.error_sns_topic.arn]
  period = 300
  statistic = "Sum"
}

resource "aws_sns_topic" "error_sns_topic" {
    name = "warn-error-occured"
}

resource "aws_sns_topic_subscription" "email_great_quote" {
  for_each = toset("${var.emails}")
  topic_arn = aws_sns_topic.error_sns_topic.arn
  protocol  = "email"
  endpoint = each.value
}
