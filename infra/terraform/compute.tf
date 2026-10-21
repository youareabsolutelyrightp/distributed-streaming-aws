data "aws_ssm_parameter" "al2023_arm64" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

data "aws_s3_object" "release" {
  count  = var.create_compute ? 1 : 0
  bucket = aws_s3_bucket.artifacts.id
  key    = var.release_object_key

  lifecycle {
    precondition {
      condition     = length(trimspace(var.release_object_key)) > 0
      error_message = "release_object_key is required when create_compute is true."
    }
    precondition {
      condition     = length(trimspace(var.deployment_expires_at)) > 0
      error_message = "deployment_expires_at is required when create_compute is true."
    }
    postcondition {
      condition     = lookup(self.metadata, "sha256", "") == var.release_sha256
      error_message = "The release object metadata does not match release_sha256."
    }
  }
}

resource "aws_instance" "node" {
  for_each = local.node_config

  ami                                  = data.aws_ssm_parameter.al2023_arm64.value
  instance_type                        = each.value.instance_type
  subnet_id                            = aws_subnet.main.id
  private_ip                           = each.value.private_ip
  vpc_security_group_ids               = [aws_security_group.cluster.id]
  iam_instance_profile                 = each.key == "control" ? aws_iam_instance_profile.control.name : aws_iam_instance_profile.cluster.name
  associate_public_ip_address          = true
  source_dest_check                    = true
  instance_initiated_shutdown_behavior = "stop"

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
    instance_metadata_tags      = "enabled"
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = each.value.root_volume_gib
    iops                  = each.value.root_volume_iops
    throughput            = each.value.root_volume_throughput
    encrypted             = true
    delete_on_termination = true
  }

  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/../cloud-init/bootstrap.sh.tftpl", {
    aws_region              = var.aws_region
    artifact_bucket         = aws_s3_bucket.artifacts.id
    release_object_key      = var.release_object_key
    release_sha256          = var.release_sha256
    deployment_id           = var.deployment_id
    profile                 = "release"
    node_name               = each.key
    node_role               = each.value.role
    private_ip              = each.value.private_ip
    control_address         = local.control_address
    database_address        = local.database_address
    database_password       = random_password.database.result
    kafka_brokers           = join(",", local.kafka_nodes)
    kafka_controller_voters = local.kafka_controller_voters
    kafka_node_id           = try(each.value.kafka_node_id, 1)
    kafka_replication       = local.topology_config.kafka_replication_factor
    kafka_minimum_isr       = local.topology_config.kafka_minimum_in_sync_replicas
    input_partitions        = local.topology_config.input_partitions
    expected_taskmanagers   = local.expected_taskmanagers
    ttl_hours               = var.ttl_hours
  })

  tags = {
    Name      = "${local.project_name}-${var.deployment_id}-${each.key}"
    NodeName  = each.key
    NodeRole  = each.value.role
    ExpiresAt = var.deployment_expires_at
  }

  depends_on = [
    data.aws_s3_object.release,
    aws_iam_role_policy.artifacts,
    aws_iam_role_policy.control_artifacts,
    aws_iam_role_policy.control_evidence_and_cleanup,
    aws_iam_role_policy_attachment.ssm,
    aws_iam_role_policy_attachment.control_ssm,
  ]
}

resource "aws_scheduler_schedule" "terminate" {
  count                        = var.create_compute ? 1 : 0
  name_prefix                  = "taxi-${substr(var.deployment_id, 0, 24)}-"
  group_name                   = "default"
  description                  = "Terminate temporary benchmark instances at the approved deadline."
  schedule_expression          = "at(${formatdate("YYYY-MM-DD'T'hh:mm:ss", local.safeguard_time)})"
  schedule_expression_timezone = "UTC"
  action_after_completion      = "DELETE"

  flexible_time_window { mode = "OFF" }

  target {
    arn      = "arn:${data.aws_partition.current.partition}:scheduler:::aws-sdk:ec2:terminateInstances"
    role_arn = aws_iam_role.safeguard[0].arn
    input    = jsonencode({ InstanceIds = [for node in aws_instance.node : node.id] })

    retry_policy {
      maximum_event_age_in_seconds = 3600
      maximum_retry_attempts       = 10
    }
  }

  depends_on = [aws_iam_role_policy.safeguard]
}
