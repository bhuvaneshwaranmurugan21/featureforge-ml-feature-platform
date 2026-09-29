output "artifact_bucket" {
  value = aws_s3_bucket.artifacts.bucket
}

output "offline_bucket" {
  value = aws_s3_bucket.offline.bucket
}

output "evidence_bucket" {
  value = aws_s3_bucket.evidence.bucket
}

output "control_table" {
  value = aws_dynamodb_table.control.name
}

output "online_table" {
  value = aws_dynamodb_table.online.name
}

output "glue_job_name" {
  value = aws_glue_job.offline.name
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.materialization.arn
}

output "github_plan_role_arn" {
  value = aws_iam_role.github_plan.arn
}
