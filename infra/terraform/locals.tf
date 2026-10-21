data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  project_name          = "reconstructed-foil-benchmark"
  release_config        = jsondecode(file("${path.module}/../../config/release.json"))
  topology_config       = local.release_config.topology
  node_config           = var.create_compute ? local.topology_config.nodes : {}
  expected_taskmanagers = local.topology_config.taskmanagers
  availability_zone     = data.aws_availability_zones.available.names[0]
  artifact_bucket_base = lower(join("-", [
    "foil-benchmark",
    data.aws_caller_identity.current.account_id,
    var.aws_region,
    var.deployment_id,
  ]))
  artifact_bucket_name = "${substr(local.artifact_bucket_base, 0, 54)}-${substr(sha256(local.artifact_bucket_base), 0, 8)}"
  control_address      = local.topology_config.nodes.control.private_ip
  database_address     = local.topology_config.nodes.database.private_ip
  kafka_nodes = [
    for name, node in local.topology_config.nodes : "${node.private_ip}:9092" if node.role == "kafka"
  ]
  kafka_controller_voters = join(",", [
    for name, node in local.topology_config.nodes : "${node.kafka_node_id}@${node.private_ip}:9093" if node.role == "kafka"
  ])
  safeguard_time = var.deployment_expires_at == "" ? "2099-01-01T00:00:00Z" : var.deployment_expires_at
  common_tags = {
    Project    = local.project_name
    Deployment = var.deployment_id
    Profile    = "release"
    Owner      = var.owner_tag
    ManagedBy  = "terraform"
    AutoRemove = "true"
    DataClass  = "public-foil"
  }
}
