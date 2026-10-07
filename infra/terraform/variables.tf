variable "aws_region" {
  type        = string
  description = "Explicit authorized AWS region; no default prevents accidental cross-region planning."

  validation {
    condition     = can(regex("^[a-z]{2}(-gov)?-[a-z]+-[0-9]+$", var.aws_region))
    error_message = "aws_region must be one explicit AWS region identifier"
  }
}

variable "admission_authority" {
  description = "Future separately approved, immutable admission object. Null keeps execution ineligible."
  type = object({
    bucket     = string
    key        = string
    version_id = string
    sha256     = string
  })
  default = null
  validation {
    condition = var.admission_authority == null ? true : (
      startswith(var.admission_authority.key, "admission/") &&
      length(var.admission_authority.version_id) > 0 &&
      can(regex("^[a-f0-9]{64}$", var.admission_authority.sha256))
    )
    error_message = "Admission requires an admission/ key, exact version, and SHA-256."
  }
}

variable "environment" {
  type        = string
  description = "Bounded environment namespace."
  default     = "stage6"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,15}$", var.environment))
    error_message = "environment must be a 2-16 character lowercase identifier"
  }
}

variable "run_id" {
  type        = string
  description = "Immutable bounded-run identifier included in every resource name."

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{2,15}$", var.run_id))
    error_message = "run_id must be a 3-16 character lowercase identifier"
  }
}

variable "control_worker_zip_path" {
  type        = string
  description = "Path to the deterministic, allowlisted control-worker archive."
  default     = "../../build/stage6/featureforge-control-worker.zip"
}

variable "glue_library_zip_path" {
  type        = string
  description = "Path to the deterministic, allowlisted Glue library archive."
  default     = "../../build/stage6/featureforge-glue-library.zip"
}

variable "glue_script_path" {
  type        = string
  description = "Path to the reviewed Glue entry point."
  default     = "../../jobs/glue_point_in_time.py"
}

variable "glue_version" {
  type        = string
  description = "Glue runtime compatible with the Stage 6 Spark 3.5 contract."
  default     = "5.0"

  validation {
    condition     = var.glue_version == "5.0"
    error_message = "Stage 6 requires Glue 5.0"
  }
}

variable "max_concurrent_runs" {
  type        = number
  description = "Stage 6 permits one managed Glue run at a time."
  default     = 1

  validation {
    condition     = var.max_concurrent_runs == 1
    error_message = "Stage 6 max_concurrent_runs must remain one"
  }
}

variable "runtime_execution_enabled" {
  type        = bool
  description = "Keep the Lambda entry point fail-closed until a separate managed-run authorization."
  default     = false

  validation {
    condition     = var.runtime_execution_enabled == false
    error_message = "Stage 6 is plan-only; runtime execution must remain disabled"
  }
}

variable "alarm_topic_arn" {
  type        = string
  description = "Optional SNS alarm topic; empty keeps notification delivery unconfigured."
  default     = ""
}

variable "github_oidc_provider_arn" {
  type        = string
  description = "Existing account-local GitHub OIDC provider ARN."
}

variable "github_repository" {
  type        = string
  description = "Exact repository permitted to assume the plan-only role."
  default     = "bhuvaneshwaranmurugan21/featureforge-ml-feature-platform"

  validation {
    condition     = var.github_repository == "bhuvaneshwaranmurugan21/featureforge-ml-feature-platform"
    error_message = "Stage 6 plan trust is restricted to the FeatureForge repository"
  }
}

variable "github_repository_owner_id" {
  type        = string
  description = "Immutable GitHub owner identifier used in the OIDC subject."
  default     = "276895096"

  validation {
    condition     = var.github_repository_owner_id == "276895096"
    error_message = "Stage 6 is restricted to the immutable FeatureForge owner identifier"
  }
}

variable "github_repository_id" {
  type        = string
  description = "Immutable GitHub repository identifier used in the OIDC subject."
  default     = "1332971230"

  validation {
    condition     = var.github_repository_id == "1332971230"
    error_message = "Stage 6 is restricted to the immutable FeatureForge repository identifier"
  }
}

variable "github_environment" {
  type        = string
  description = "Protected GitHub environment permitted to assume the plan-only role."
  default     = "featureforge-stage6-plan"

  validation {
    condition     = var.github_environment == "featureforge-stage6-plan"
    error_message = "Stage 6 requires the protected featureforge-stage6-plan environment"
  }
}
