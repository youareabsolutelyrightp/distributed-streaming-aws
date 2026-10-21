output "artifact_bucket" {
  description = "Private S3 bucket for releases, checkpoints, and evidence."
  value       = aws_s3_bucket.artifacts.id
}

output "profile" {
  value = "release"
}

output "availability_zone" {
  value = local.availability_zone
}

output "topology_configuration" {
  value = local.topology_config
}

output "taskmanager_instance_type" {
  value = local.topology_config.taskmanager_instance_type
}

output "expected_taskmanagers" {
  value = local.expected_taskmanagers
}

output "instance_ids" {
  value = { for name, node in aws_instance.node : name => node.id }
}

output "private_addresses" {
  value = { for name, node in aws_instance.node : name => node.private_ip }
}

output "control_instance_id" {
  value = try(aws_instance.node["control"].id, null)
}

output "database_password" {
  value     = random_password.database.result
  sensitive = true
}

output "deployment_expires_at" {
  value = var.deployment_expires_at
}
