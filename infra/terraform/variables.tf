variable "aws_region" {
  type        = string
  description = "AWS region for the reference deployment."
  default     = "ap-south-1"
}

variable "environment" {
  type        = string
  description = "Short environment name."
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod"
  }
}

variable "materialization_schedule" {
  type        = string
  description = "EventBridge schedule expression for materialization."
  default     = "rate(1 hour)"
}

variable "enable_schedule" {
  type        = bool
  description = "Enable scheduled orchestration only after adapters are configured."
  default     = false
}

variable "alarm_topic_arn" {
  type        = string
  description = "Optional SNS topic ARN for alarm actions."
  default     = ""
}
