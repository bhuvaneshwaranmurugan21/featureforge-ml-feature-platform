data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "control_worker" {
  name               = "${local.name}-control-worker"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "control_worker" {
  statement {
    sid = "ExactManagedBuckets"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:PutObject"
    ]
    resources = [
      "${aws_s3_bucket.offline.arn}/*",
      "${aws_s3_bucket.evidence.arn}/*"
    ]
  }

  statement {
    sid       = "BoundedBucketListing"
    actions   = ["s3:ListBucket", "s3:GetBucketVersioning"]
    resources = [aws_s3_bucket.offline.arn, aws_s3_bucket.evidence.arn]
  }

  statement {
    sid = "ManagedDynamoDB"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:Query",
      "dynamodb:TransactWriteItems",
      "dynamodb:UpdateItem"
    ]
    resources = [aws_dynamodb_table.control.arn, aws_dynamodb_table.online.arn]
  }

  statement {
    sid = "ManagedKmsKey"
    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GenerateDataKey"
    ]
    resources = [aws_kms_key.platform.arn]
  }

  statement {
    sid       = "WorkerLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.control_worker.arn}:*"]
  }
}

resource "aws_iam_role_policy" "control_worker" {
  role   = aws_iam_role.control_worker.id
  policy = data.aws_iam_policy_document.control_worker.json
}

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.name}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

data "aws_iam_policy_document" "glue" {
  statement {
    sid       = "ReadArtifacts"
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.artifacts.arn}/*"]
  }

  statement {
    sid = "ReadWriteManagedData"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:PutObject"
    ]
    resources = [
      "${aws_s3_bucket.offline.arn}/*",
      "${aws_s3_bucket.evidence.arn}/*"
    ]
  }

  statement {
    sid     = "ListManagedBuckets"
    actions = ["s3:GetBucketLocation", "s3:GetBucketVersioning", "s3:ListBucket"]
    resources = [
      aws_s3_bucket.artifacts.arn,
      aws_s3_bucket.evidence.arn,
      aws_s3_bucket.offline.arn
    ]
  }

  statement {
    sid = "ManagedKmsKey"
    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GenerateDataKey"
    ]
    resources = [aws_kms_key.platform.arn]
  }

  statement {
    sid = "GlueLogs"
    actions = [
      "logs:AssociateKmsKey",
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:${data.aws_partition.current.partition}:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/jobs/*"]
  }
}

resource "aws_iam_role_policy" "glue" {
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue.json
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
    sid       = "InvokeControlWorker"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.control_worker.arn, "${aws_lambda_function.control_worker.arn}:*"]
  }

  statement {
    sid       = "RunExactGlueJob"
    actions   = ["glue:BatchStopJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:StartJobRun"]
    resources = [aws_glue_job.offline.arn]
  }

  # Step Functions log-delivery APIs do not support resource-level permissions.
  statement {
    sid = "StateMachineLogDelivery"
    actions = [
      "logs:CreateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:DescribeLogGroups",
      "logs:DescribeResourcePolicies",
      "logs:GetLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:UpdateLogDelivery"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "states" {
  role   = aws_iam_role.states.id
  policy = data.aws_iam_policy_document.states.json
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

data "aws_iam_policy_document" "events" {
  statement {
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.materialization.arn]
  }
}

resource "aws_iam_role_policy" "events" {
  role   = aws_iam_role.events.id
  policy = data.aws_iam_policy_document.events.json
}

data "aws_iam_policy_document" "github_plan_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [var.github_oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repository}:environment:${var.github_environment}"]
    }
  }
}

resource "aws_iam_role" "github_plan" {
  name               = "${local.name}-github-plan"
  assume_role_policy = data.aws_iam_policy_document.github_plan_assume.json
}

data "aws_iam_policy_document" "github_plan_readonly" {
  statement {
    sid = "FeatureForgeQualificationReads"
    actions = [
      "cloudwatch:DescribeAlarms",
      "dynamodb:DescribeContinuousBackups",
      "dynamodb:DescribeTable",
      "dynamodb:DescribeTimeToLive",
      "dynamodb:ListTagsOfResource",
      "events:DescribeRule",
      "events:ListTargetsByRule",
      "glue:GetDatabase",
      "glue:GetJob",
      "glue:GetTags",
      "iam:GetRole",
      "iam:GetRolePolicy",
      "iam:ListAttachedRolePolicies",
      "iam:ListRolePolicies",
      "iam:SimulatePrincipalPolicy",
      "kms:DescribeKey",
      "kms:ListAliases",
      "lambda:GetFunction",
      "lambda:GetFunctionConfiguration",
      "lambda:ListTags",
      "logs:DescribeLogGroups",
      "budgets:ViewBudget",
      "ce:GetCostAndUsage",
      "pricing:GetProducts",
      "s3:GetBucketEncryption",
      "s3:GetBucketLocation",
      "s3:GetBucketPolicyStatus",
      "s3:GetBucketPublicAccessBlock",
      "s3:GetBucketTagging",
      "s3:GetBucketVersioning",
      "s3:ListBucketVersions",
      "servicequotas:GetServiceQuota",
      "servicequotas:ListServiceQuotas",
      "states:DescribeStateMachine",
      "states:ListTagsForResource",
      "sts:GetCallerIdentity"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "github_plan_readonly" {
  role   = aws_iam_role.github_plan.id
  policy = data.aws_iam_policy_document.github_plan_readonly.json
}
