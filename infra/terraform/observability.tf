resource "aws_cloudwatch_metric_alarm" "workflow_failed" {
  alarm_name          = "${local.name}-workflow-failed"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  dimensions = {
    StateMachineArn = aws_sfn_state_machine.materialization.arn
  }
}

resource "aws_cloudwatch_metric_alarm" "workflow_timed_out" {
  alarm_name          = "${local.name}-workflow-timed-out"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsTimedOut"
  namespace           = "AWS/States"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  dimensions = {
    StateMachineArn = aws_sfn_state_machine.materialization.arn
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${local.name}-lambda-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  dimensions = {
    FunctionName = aws_lambda_function.control_worker.function_name
  }
}

resource "aws_cloudwatch_dashboard" "managed_run" {
  dashboard_name = "${local.name}-managed-run"
  dashboard_body = jsonencode({
    widgets = [{
      type   = "metric"
      x      = 0
      y      = 0
      width  = 12
      height = 6
      properties = {
        title  = "FeatureForge bounded run failures"
        region = var.aws_region
        view   = "timeSeries"
        metrics = [
          ["AWS/States", "ExecutionsFailed", "StateMachineArn", aws_sfn_state_machine.materialization.arn],
          ["AWS/States", "ExecutionsTimedOut", "StateMachineArn", aws_sfn_state_machine.materialization.arn],
          ["AWS/Lambda", "Errors", "FunctionName", aws_lambda_function.control_worker.function_name]
        ]
      }
    }]
  })
}
