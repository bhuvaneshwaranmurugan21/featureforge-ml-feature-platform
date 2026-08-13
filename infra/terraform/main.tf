data "aws_caller_identity" "current" {}

resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  name          = "featureforge-${var.environment}"
  bucket_prefix = "${local.name}-${data.aws_caller_identity.current.account_id}-${random_id.suffix.hex}"
  alarm_actions = var.alarm_topic_arn == "" ? [] : [var.alarm_topic_arn]
}

resource "aws_kms_key" "platform" {
  description             = "FeatureForge offline, online, registry, and evidence encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true
}

resource "aws_kms_alias" "platform" {
  name          = "alias/${local.name}"
  target_key_id = aws_kms_key.platform.key_id
}

resource "aws_s3_bucket" "offline" {
  bucket        = "${local.bucket_prefix}-offline"
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket" "evidence" {
  bucket        = "${local.bucket_prefix}-evidence"
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_versioning" "offline" {
  bucket = aws_s3_bucket.offline.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_versioning" "evidence" {
  bucket = aws_s3_bucket.evidence.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "offline" {
  bucket = aws_s3_bucket.offline.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.platform.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "evidence" {
  bucket = aws_s3_bucket.evidence.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.platform.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "offline" {
  bucket                  = aws_s3_bucket.offline.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "evidence" {
  bucket                  = aws_s3_bucket.evidence.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_glue_catalog_database" "features" {
  name = replace("${local.name}-offline", "-", "_")
}

resource "aws_dynamodb_table" "registry" {
  name         = "${local.name}-registry"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "definition_id"

  attribute {
    name = "definition_id"
    type = "S"
  }

  point_in_time_recovery { enabled = true }
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.platform.arn
  }
}

resource "aws_dynamodb_table" "generations" {
  name         = "${local.name}-generations"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "generation_id"

  attribute {
    name = "generation_id"
    type = "S"
  }

  point_in_time_recovery { enabled = true }
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.platform.arn
  }
}

resource "aws_dynamodb_table" "online" {
  name         = "${local.name}-online"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "entity_id"
  range_key    = "generation_feature"

  attribute {
    name = "entity_id"
    type = "S"
  }
  attribute {
    name = "generation_feature"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
  point_in_time_recovery { enabled = true }
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.platform.arn
  }
}

resource "aws_dynamodb_table" "active_pointer" {
  name         = "${local.name}-active-pointer"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "product_id"

  attribute {
    name = "product_id"
    type = "S"
  }

  point_in_time_recovery { enabled = true }
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.platform.arn
  }
}

resource "aws_cloudwatch_log_group" "orchestration" {
  name              = "/aws/vendedlogs/states/${local.name}"
  retention_in_days = 30
  kms_key_id        = aws_kms_key.platform.arn
}

data "aws_iam_policy_document" "states_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "states" {
  name               = "${local.name}-states"
  assume_role_policy = data.aws_iam_policy_document.states_assume.json
}

data "aws_iam_policy_document" "states" {
  statement {
    actions = [
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "states" {
  role   = aws_iam_role.states.id
  policy = data.aws_iam_policy_document.states.json
}

resource "aws_sfn_state_machine" "materialization" {
  name     = "${local.name}-materialization-contract"
  role_arn = aws_iam_role.states.arn

  logging_configuration {
    include_execution_data = true
    level                  = "ALL"
    log_destination        = "${aws_cloudwatch_log_group.orchestration.arn}:*"
  }

  definition = jsonencode({
    Comment = "Control-flow contract; replace Pass adapters only after managed verification"
    StartAt = "ValidateManifest"
    States = {
      ValidateManifest      = { Type = "Pass", Next = "MaterializeGeneration" }
      MaterializeGeneration = { Type = "Pass", Next = "ParityGate" }
      ParityGate = {
        Type    = "Choice"
        Choices = [{ Variable = "$.parity_matched", BooleanEquals = true, Next = "Ready" }]
        Default = "Quarantined"
      }
      Ready       = { Type = "Succeed" }
      Quarantined = { Type = "Fail", Error = "ParityFailed" }
    }
  })
}

data "aws_iam_policy_document" "events_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "events" {
  name               = "${local.name}-events"
  assume_role_policy = data.aws_iam_policy_document.events_assume.json
}

resource "aws_iam_role_policy" "events" {
  role = aws_iam_role.events.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = aws_sfn_state_machine.materialization.arn
    }]
  })
}

resource "aws_cloudwatch_event_rule" "materialization" {
  name                = "${local.name}-materialization"
  description         = "Start an isolated feature generation"
  schedule_expression = var.materialization_schedule
  state               = var.enable_schedule ? "ENABLED" : "DISABLED"
}

resource "aws_cloudwatch_event_target" "materialization" {
  rule     = aws_cloudwatch_event_rule.materialization.name
  arn      = aws_sfn_state_machine.materialization.arn
  role_arn = aws_iam_role.events.arn
  input    = jsonencode({ parity_matched = false, adapter_configured = false })
}

resource "aws_cloudwatch_metric_alarm" "parity_mismatch" {
  alarm_name          = "${local.name}-parity-mismatch"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ParityMismatchCount"
  namespace           = "FeatureForge"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
}
