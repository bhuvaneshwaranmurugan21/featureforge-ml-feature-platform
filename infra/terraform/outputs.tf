output "offline_bucket" {
  value = aws_s3_bucket.offline.bucket
}

output "evidence_bucket" {
  value = aws_s3_bucket.evidence.bucket
}

output "online_table" {
  value = aws_dynamodb_table.online.name
}

output "registry_table" {
  value = aws_dynamodb_table.registry.name
}

output "active_pointer_table" {
  value = aws_dynamodb_table.active_pointer.name
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.materialization.arn
}
