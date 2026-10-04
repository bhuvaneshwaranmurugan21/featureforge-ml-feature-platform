resource "aws_cloudwatch_log_group" "control_worker" {
  name              = local.managed_log_group_names.control_worker
  retention_in_days = 7
  kms_key_id        = aws_kms_key.platform.arn
}

resource "aws_cloudwatch_log_group" "orchestration" {
  name              = local.managed_log_group_names.orchestration
  retention_in_days = 7
  kms_key_id        = aws_kms_key.platform.arn
}

resource "aws_cloudwatch_log_group" "glue_error" {
  name              = local.managed_log_group_names.glue_error
  retention_in_days = 7
  kms_key_id        = aws_kms_key.platform.arn
}

resource "aws_cloudwatch_log_group" "glue_output" {
  name              = local.managed_log_group_names.glue_output
  retention_in_days = 7
  kms_key_id        = aws_kms_key.platform.arn
}

resource "aws_glue_security_configuration" "managed" {
  name = local.glue_security_name
  encryption_configuration {
    cloudwatch_encryption {
      cloudwatch_encryption_mode = "SSE-KMS"
      kms_key_arn                = aws_kms_key.platform.arn
    }
    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "CSE-KMS"
      kms_key_arn                   = aws_kms_key.platform.arn
    }
    s3_encryption {
      s3_encryption_mode = "SSE-KMS"
      kms_key_arn        = aws_kms_key.platform.arn
    }
  }
}

resource "aws_glue_job" "offline" {
  name                   = "${local.name}-offline"
  role_arn               = aws_iam_role.glue.arn
  glue_version           = var.glue_version
  worker_type            = "G.1X"
  number_of_workers      = 2
  timeout                = 15
  max_retries            = 0
  execution_class        = "STANDARD"
  security_configuration = aws_glue_security_configuration.managed.name

  execution_property {
    max_concurrent_runs = var.max_concurrent_runs
  }

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_object.glue_script.bucket}/${aws_s3_object.glue_script.key}"
  }

  default_arguments = {
    "--additional-python-modules"        = ""
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--enable-metrics"                   = "true"
    "--custom-logGroup-prefix"           = local.glue_log_group_prefix
    "--extra-py-files"                   = "s3://${aws_s3_object.glue_library.bucket}/${aws_s3_object.glue_library.key}"
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${aws_s3_bucket.offline.bucket}/tmp/${var.run_id}/"
  }

  depends_on = [aws_cloudwatch_log_group.glue_error, aws_cloudwatch_log_group.glue_output]
}

resource "aws_lambda_function" "control_worker" {
  function_name                  = "${local.name}-control-worker"
  role                           = aws_iam_role.control_worker.arn
  handler                        = "featureforge.control_worker.lambda_handler"
  runtime                        = "python3.12"
  architectures                  = ["x86_64"]
  memory_size                    = 512
  timeout                        = 300
  reserved_concurrent_executions = 1
  s3_bucket                      = aws_s3_object.control_worker.bucket
  s3_key                         = aws_s3_object.control_worker.key
  s3_object_version              = aws_s3_object.control_worker.version_id
  source_code_hash               = filebase64sha256(var.control_worker_zip_path)

  environment {
    variables = local.common_environment
  }

  tracing_config { mode = "Active" }

  depends_on = [aws_cloudwatch_log_group.control_worker]
}

locals {
  lambda_retry = [
    {
      ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "Lambda.TooManyRequestsException"]
      IntervalSeconds = 2
      MaxAttempts     = 3
      BackoffRate     = 2
    }
  ]
  catch_quarantine = [{ ErrorEquals = ["States.ALL"], Next = "Quarantined", ResultPath = "$.failure" }]
}

resource "aws_sfn_state_machine" "materialization" {
  name     = "${local.name}-materialization"
  role_arn = aws_iam_role.states.arn
  type     = "STANDARD"

  logging_configuration {
    include_execution_data = false
    level                  = "ERROR"
    log_destination        = "${aws_cloudwatch_log_group.orchestration.arn}:*"
  }

  tracing_configuration { enabled = true }

  definition = jsonencode({
    Comment        = "FeatureForge bounded, parity-gated managed materialization"
    StartAt        = "ValidateManifest"
    TimeoutSeconds = 3600
    States = {
      ValidateManifest = {
        Type     = "Task"
        Resource = "arn:${data.aws_partition.current.partition}:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.control_worker.arn
          Payload = {
            action                 = "VALIDATE"
            "manifest.$"           = "$.manifest"
            "manifest_authority.$" = "$.manifest_authority"
            payload = {
              "execution_id.$" = "$$.Execution.Id"
            }
          }
        }
        ResultPath = "$.validation_result"
        Retry      = local.lambda_retry
        Catch      = local.catch_quarantine
        Next       = "BuildOfflineGeneration"
      }
      BuildOfflineGeneration = {
        Type     = "Task"
        Resource = "arn:${data.aws_partition.current.partition}:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.control_worker.arn
          Payload = {
            action                 = "START_GLUE"
            "manifest.$"           = "$.manifest"
            "manifest_authority.$" = "$.manifest_authority"
            payload = {
              "execution_id.$" = "$$.Execution.Id"
            }
          }
        }
        ResultSelector = {
          "JobName.$"  = "$.Payload.result.JobName"
          "JobRunId.$" = "$.Payload.result.JobRunId"
          "receipt.$"  = "$.Payload"
        }
        ResultPath = "$.glue_result"
        Retry = [merge(local.lambda_retry[0], {
          ErrorEquals = concat(local.lambda_retry[0].ErrorEquals, ["GlueLaunchRetryable"])
        })]
        Catch = local.catch_quarantine
        Next  = "WaitForGlueCompletion"
      }
      WaitForGlueCompletion = {
        Type    = "Wait"
        Seconds = 10
        Next    = "VerifyGlueCompletion"
      }
      VerifyGlueCompletion = {
        Type     = "Task"
        Resource = "arn:${data.aws_partition.current.partition}:states:::aws-sdk:glue:getJobRun"
        Parameters = {
          JobName              = aws_glue_job.offline.name
          "RunId.$"            = "$.glue_result.JobRunId"
          PredecessorsIncluded = false
        }
        ResultPath = "$.glue_completion"
        Retry = [{
          ErrorEquals     = ["Glue.InternalServiceException", "Glue.OperationTimeoutException", "Glue.ThrottlingException"]
          IntervalSeconds = 2
          MaxAttempts     = 2
          BackoffRate     = 2
        }]
        Catch = local.catch_quarantine
        Next  = "GlueCompletionDecision"
      }
      GlueCompletionDecision = {
        Type = "Choice"
        Choices = [{
          And = [
            {
              Variable         = "$.glue_completion.JobRun.Id"
              StringEqualsPath = "$.glue_result.JobRunId"
            },
            {
              Variable         = "$.glue_completion.JobRun.JobName"
              StringEqualsPath = "$.glue_result.JobName"
            },
            {
              Variable     = "$.glue_completion.JobRun.JobRunState"
              StringEquals = "SUCCEEDED"
            }
          ]
          Next = "RecordGlueCompletion"
          }, {
          And = [
            {
              Variable         = "$.glue_completion.JobRun.Id"
              StringEqualsPath = "$.glue_result.JobRunId"
            },
            {
              Variable         = "$.glue_completion.JobRun.JobName"
              StringEqualsPath = "$.glue_result.JobName"
            },
            {
              Or = [
                { Variable = "$.glue_completion.JobRun.JobRunState", StringEquals = "STARTING" },
                { Variable = "$.glue_completion.JobRun.JobRunState", StringEquals = "RUNNING" },
                { Variable = "$.glue_completion.JobRun.JobRunState", StringEquals = "WAITING" },
                { Variable = "$.glue_completion.JobRun.JobRunState", StringEquals = "STOPPING" }
              ]
            }
          ]
          Next = "WaitForGlueCompletion"
        }]
        Default = "Quarantined"
      }
      RecordGlueCompletion = {
        Type     = "Task"
        Resource = "arn:${data.aws_partition.current.partition}:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.control_worker.arn
          Payload = {
            action                 = "RECORD_GLUE"
            "manifest.$"           = "$.manifest"
            "manifest_authority.$" = "$.manifest_authority"
            payload = {
              "glue_job_run_id.$" = "$.glue_completion.JobRun.Id"
              "state.$"           = "$.glue_completion.JobRun.JobRunState"
              job_name            = aws_glue_job.offline.name
            }
          }
        }
        ResultPath = "$.glue_receipt"
        Retry      = local.lambda_retry
        Catch      = local.catch_quarantine
        Next       = "MaterializeOnlineCandidate"
      }
      MaterializeOnlineCandidate = {
        Type     = "Task"
        Resource = "arn:${data.aws_partition.current.partition}:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.control_worker.arn
          Payload = {
            action                 = "MATERIALIZE_ONLINE"
            "manifest.$"           = "$.manifest"
            "manifest_authority.$" = "$.manifest_authority"
            payload = {
              "online_payload_authority.$" = "$.glue_receipt.Payload.result.online_payload_authority"
              "feature_set.$"              = "$.materialization_context.feature_set"
              "materialized_at.$"          = "$.materialization_context.materialized_at"
              "request_time.$"             = "$.materialization_context.request_time"
              "maximum_freshness_age.$"    = "$.materialization_context.maximum_freshness_age"
            }
          }
        }
        ResultPath = "$.online_result"
        Retry      = local.lambda_retry
        Catch      = local.catch_quarantine
        Next       = "ParityGate"
      }
      ParityGate = {
        Type     = "Task"
        Resource = "arn:${data.aws_partition.current.partition}:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.control_worker.arn
          Payload = {
            action                 = "PARITY"
            "manifest.$"           = "$.manifest"
            "manifest_authority.$" = "$.manifest_authority"
            payload = {
              "online_payload_authority.$" = "$.glue_receipt.Payload.result.online_payload_authority"
              "feature_set.$"              = "$.materialization_context.feature_set"
              "materialized_at.$"          = "$.materialization_context.materialized_at"
              "request_time.$"             = "$.materialization_context.request_time"
              "maximum_freshness_age.$"    = "$.materialization_context.maximum_freshness_age"
            }
          }
        }
        ResultPath = "$.parity_result"
        Retry      = local.lambda_retry
        Catch      = local.catch_quarantine
        Next       = "ParityDecision"
      }
      ParityDecision = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.parity_result.Payload.result.decision"
          StringEquals = "ELIGIBLE"
          Next         = "ActivateGeneration"
        }]
        Default = "Quarantined"
      }
      ActivateGeneration = {
        Type     = "Task"
        Resource = "arn:${data.aws_partition.current.partition}:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.control_worker.arn
          Payload = {
            action                 = "ACTIVATE"
            "manifest.$"           = "$.manifest"
            "manifest_authority.$" = "$.manifest_authority"
            payload = {
              "expected_generation.$"   = "$.activation_context.expected_generation"
              "expected_version.$"      = "$.activation_context.expected_version"
              "parity_receipt_digest.$" = "$.parity_result.Payload.result.parity_digest"
            }
          }
        }
        ResultPath = "$.activation_result"
        Retry      = local.lambda_retry
        Catch      = local.catch_quarantine
        Next       = "CompleteRun"
      }
      CompleteRun = {
        Type     = "Task"
        Resource = "arn:${data.aws_partition.current.partition}:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.control_worker.arn
          Payload = {
            action                 = "COMPLETE"
            "manifest.$"           = "$.manifest"
            "manifest_authority.$" = "$.manifest_authority"
            payload = {
              "admission.$"      = "$.validation_result.Payload.result"
              "output_objects.$" = "$.glue_receipt.Payload.result.output_objects"
              "task_receipts.$"  = "States.Array($.validation_result.Payload, $.glue_result.receipt, $.glue_receipt.Payload, $.online_result.Payload, $.parity_result.Payload, $.activation_result.Payload)"
            }
          }
        }
        Retry = local.lambda_retry
        Catch = local.catch_quarantine
        End   = true
      }
      Quarantined = {
        Type  = "Fail"
        Error = "FeatureForgeCandidateQuarantined"
        Cause = "A fail-closed managed-run gate rejected the candidate"
      }
    }
  })
}

resource "aws_cloudwatch_event_rule" "materialization" {
  name                = "${local.name}-manual-only"
  description         = "Intentionally disabled; bounded runs are manually admitted"
  schedule_expression = "rate(1 day)"
  state               = "DISABLED"
}

resource "aws_cloudwatch_event_target" "materialization" {
  rule     = aws_cloudwatch_event_rule.materialization.name
  arn      = aws_sfn_state_machine.materialization.arn
  role_arn = aws_iam_role.events.arn
  input    = jsonencode({ disabled = true, reason = "manual-bounded-run-only" })
}
