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
    sid       = "BudgetedExactGlueLaunch"
    actions   = ["glue:GetJob", "glue:StartJobRun"]
    resources = [aws_glue_job.offline.arn]
  }
  statement {
    sid       = "ReadImmutableAdmissionAuthority"
    actions   = ["s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.artifacts.arn}/admission/*"]
  }

  statement {
    sid       = "ReadExactExclusiveLease"
    actions   = ["s3:GetObject"]
    resources = ["arn:${data.aws_partition.current.partition}:s3:::featureforge-stage6-tfstate-${data.aws_caller_identity.current.account_id}-${var.aws_region}/leases/featureforge/stage6.json"]
  }

  statement {
    sid       = "ReadCurrentManagedQuotas"
    actions   = ["servicequotas:GetServiceQuota"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.aws_region]
    }
  }

  statement {
    sid       = "ReadGrossAccountBudgetHeadroom"
    actions   = ["budgets:ViewBudget"]
    resources = ["arn:${data.aws_partition.current.partition}:budgets::${data.aws_caller_identity.current.account_id}:budget/*"]
  }

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
    sid       = "ExactOnlineCandidateReconciliation"
    actions   = ["dynamodb:Scan"]
    resources = [aws_dynamodb_table.online.arn]
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

  # These X-Ray APIs do not support resource-level permissions.
  statement {
    sid       = "WorkerActiveTracing"
    actions   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords"]
    resources = ["*"]
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
  name               = local.glue_role_name
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

data "aws_iam_policy_document" "glue" {
  # Glue job metrics use this namespace and have no resource ARN.
  statement {
    sid       = "ExactGlueMetricNamespace"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["Glue"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.aws_region]
    }
  }

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
    sid       = "ExactGlueLogGroupConfiguration"
    actions   = ["logs:AssociateKmsKey", "logs:CreateLogGroup"]
    resources = [aws_cloudwatch_log_group.glue_error.arn, aws_cloudwatch_log_group.glue_output.arn]
  }

  statement {
    sid       = "ExactGlueLogStreams"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.glue_error.arn}:*", "${aws_cloudwatch_log_group.glue_output.arn}:*"]
  }

  statement {
    sid       = "ExactGlueLogKeyAssociation"
    actions   = ["kms:DescribeKey"]
    resources = [aws_kms_key.platform.arn]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["logs.${var.aws_region}.amazonaws.com"]
    }
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
    actions   = ["glue:GetJobRun"]
    resources = [aws_glue_job.offline.arn]
  }

  # The four service-managed tracing APIs have no resource-level permissions.
  statement {
    sid = "StateMachineActiveTracing"
    actions = [
      "xray:PutTraceSegments",
      "xray:PutTelemetryRecords",
      "xray:GetSamplingRules",
      "xray:GetSamplingTargets"
    ]
    resources = ["*"]
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
      values = [
        "repo:bhuvaneshwaranmurugan21@${var.github_repository_owner_id}/featureforge-ml-feature-platform@${var.github_repository_id}:environment:${var.github_environment}"
      ]
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
      "cloudwatch:GetDashboard",
      "dynamodb:DescribeContinuousBackups",
      "dynamodb:DescribeTable",
      "dynamodb:DescribeTimeToLive",
      "dynamodb:ListTagsOfResource",
      "events:DescribeRule",
      "events:ListTargetsByRule",
      "glue:GetDatabase",
      "glue:GetJob",
      "glue:GetSecurityConfiguration",
      "glue:GetTags",
      "iam:GetRole",
      "iam:GetOpenIDConnectProvider",
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
      "s3:GetEncryptionConfiguration",
      "s3:GetBucketLocation",
      "s3:GetBucketOwnershipControls",
      "s3:GetBucketPolicyStatus",
      "s3:GetBucketPublicAccessBlock",
      "s3:GetBucketTagging",
      "s3:GetBucketVersioning",
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:ListBucket",
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
