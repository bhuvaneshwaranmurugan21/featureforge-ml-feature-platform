data "aws_partition" "current" {}

data "aws_caller_identity" "current" {}

locals {
  name                  = "featureforge-${var.environment}-${var.run_id}"
  bucket_prefix         = "${local.name}-${data.aws_caller_identity.current.account_id}"
  alarm_actions         = var.alarm_topic_arn == "" ? [] : [var.alarm_topic_arn]
  glue_security_name    = "${local.name}-security"
  glue_role_name        = "${local.name}-glue"
  glue_log_group_prefix = "/aws-glue/jobs/${local.name}"
  glue_log_group_base   = "${local.glue_log_group_prefix}/${local.glue_security_name}-role/${local.glue_role_name}"
  managed_log_group_names = {
    control_worker = "/aws/lambda/${local.name}-control-worker"
    orchestration  = "/aws/vendedlogs/states/${local.name}"
    glue_error     = "${local.glue_log_group_base}/error"
    glue_output    = "${local.glue_log_group_base}/output"
  }
  common_environment = {
    ADMISSION_AUTHORITY_JSON    = var.admission_authority == null ? "" : jsonencode(var.admission_authority)
    CONTROL_TABLE               = aws_dynamodb_table.control.name
    EVIDENCE_BUCKET             = aws_s3_bucket.evidence.bucket
    EXPECTED_ACCOUNT            = data.aws_caller_identity.current.account_id
    FEATUREFORGE_STAGE6_ENABLED = tostring(var.runtime_execution_enabled)
    GLUE_JOB_NAME               = aws_glue_job.offline.name
    KMS_KEY_ARN                 = aws_kms_key.platform.arn
    OFFLINE_BUCKET              = aws_s3_bucket.offline.bucket
    ONLINE_TABLE                = aws_dynamodb_table.online.name
    RUN_ID                      = var.run_id
  }
}

data "aws_iam_policy_document" "kms" {
  statement {
    sid       = "AccountAdministration"
    actions   = ["kms:*"]
    resources = ["*"]
    principals {
      type        = "AWS"
      identifiers = ["arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root"]
    }
  }

  statement {
    sid = "CloudWatchLogsEncryption"
    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GenerateDataKey*",
      "kms:ReEncrypt*",
      "kms:DescribeKey"
    ]
    resources = ["*"]
    principals {
      type        = "Service"
      identifiers = ["logs.${var.aws_region}.amazonaws.com"]
    }
    condition {
      test     = "ArnEquals"
      variable = "kms:EncryptionContext:aws:logs:arn"
      values   = [for name in values(local.managed_log_group_names) : "arn:${data.aws_partition.current.partition}:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:${name}"]
    }
  }
}

resource "aws_kms_key" "platform" {
  customer_master_key_spec = "SYMMETRIC_DEFAULT"
  description              = "FeatureForge Stage 6 managed-runtime encryption"
  deletion_window_in_days  = 7
  enable_key_rotation      = true
  key_usage                = "ENCRYPT_DECRYPT"
  policy                   = data.aws_iam_policy_document.kms.json
}

resource "aws_kms_alias" "platform" {
  name          = "alias/${local.name}"
  target_key_id = aws_kms_key.platform.key_id
}

resource "aws_s3_bucket" "artifacts" {
  bucket        = "${local.bucket_prefix}-artifacts"
  force_destroy = true
}

resource "aws_s3_bucket" "offline" {
  bucket        = "${local.bucket_prefix}-offline"
  force_destroy = true
}

resource "aws_s3_bucket" "evidence" {
  bucket        = "${local.bucket_prefix}-evidence"
  force_destroy = true
}

locals {
  managed_buckets = {
    artifacts = aws_s3_bucket.artifacts.id
    evidence  = aws_s3_bucket.evidence.id
    offline   = aws_s3_bucket.offline.id
  }
}

resource "aws_s3_bucket_versioning" "managed" {
  for_each = local.managed_buckets
  bucket   = each.value
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_lifecycle_configuration" "managed" {
  for_each = local.managed_buckets
  bucket   = each.value

  rule {
    id     = "stage6-thirty-day-cost-horizon"
    status = "Enabled"

    filter {}

    expiration {
      days = 30
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  depends_on = [aws_s3_bucket_versioning.managed]
}

resource "aws_s3_bucket_server_side_encryption_configuration" "managed" {
  for_each = local.managed_buckets
  bucket   = each.value
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.platform.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "managed" {
  for_each                = local.managed_buckets
  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "control_worker" {
  bucket                 = aws_s3_bucket.artifacts.id
  key                    = "${var.run_id}/${filebase64sha256(var.control_worker_zip_path)}/featureforge-control-worker.zip"
  source                 = var.control_worker_zip_path
  source_hash            = filebase64sha256(var.control_worker_zip_path)
  server_side_encryption = "aws:kms"
  kms_key_id             = aws_kms_key.platform.arn
  depends_on             = [aws_s3_bucket_versioning.managed]
}

resource "aws_s3_object" "glue_library" {
  bucket                 = aws_s3_bucket.artifacts.id
  key                    = "${var.run_id}/${filebase64sha256(var.glue_library_zip_path)}/featureforge-glue-library.zip"
  source                 = var.glue_library_zip_path
  source_hash            = filebase64sha256(var.glue_library_zip_path)
  server_side_encryption = "aws:kms"
  kms_key_id             = aws_kms_key.platform.arn
  depends_on             = [aws_s3_bucket_versioning.managed]
}

resource "aws_s3_object" "glue_script" {
  bucket                 = aws_s3_bucket.artifacts.id
  key                    = "${var.run_id}/${filesha256(var.glue_script_path)}/glue_point_in_time.py"
  source                 = var.glue_script_path
  source_hash            = filesha256(var.glue_script_path)
  server_side_encryption = "aws:kms"
  kms_key_id             = aws_kms_key.platform.arn
  depends_on             = [aws_s3_bucket_versioning.managed]
}

resource "aws_glue_catalog_database" "features" {
  name = replace("${local.name}-offline", "-", "_")
}

resource "aws_dynamodb_table" "control" {
  name         = "${local.name}-control"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "PK"
  range_key    = "SK"

  attribute {
    name = "PK"
    type = "S"
  }
  attribute {
    name = "SK"
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
  hash_key     = "PK"
  range_key    = "SK"

  attribute {
    name = "PK"
    type = "S"
  }
  attribute {
    name = "SK"
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
