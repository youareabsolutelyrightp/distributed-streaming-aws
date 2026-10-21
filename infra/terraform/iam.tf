resource "aws_iam_role" "instance" {
  name_prefix = "taxi-${substr(var.deployment_id, 0, 24)}-node-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.instance.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "artifacts" {
  name = "artifact-access"
  role = aws_iam_role.instance.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:ListBucketMultipartUploads",
        ]
        Resource = aws_s3_bucket.artifacts.arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts",
        ]
        Resource = "${aws_s3_bucket.artifacts.arn}/*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "cluster" {
  name_prefix = "${local.project_name}-${var.deployment_id}-"
  role        = aws_iam_role.instance.name
}

resource "aws_iam_role" "control" {
  name_prefix        = "taxi-${substr(var.deployment_id, 0, 24)}-control-"
  assume_role_policy = aws_iam_role.instance.assume_role_policy
}

resource "aws_iam_role_policy_attachment" "control_ssm" {
  role       = aws_iam_role.control.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "control_artifacts" {
  name   = "artifact-access"
  role   = aws_iam_role.control.id
  policy = aws_iam_role_policy.artifacts.policy
}

resource "aws_iam_instance_profile" "control" {
  name_prefix = "${local.project_name}-${var.deployment_id}-control-"
  role        = aws_iam_role.control.name
}

resource "aws_iam_role_policy" "control_evidence_and_cleanup" {
  count = var.create_compute ? 1 : 0
  name  = "deployment-evidence-and-cleanup"
  role  = aws_iam_role.control.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ec2:DescribeInstances",
          "ec2:DescribeInstanceTypes",
          "ec2:DescribeVolumes",
          "ssm:GetCommandInvocation",
          "ssm:ListCommandInvocations"
        ]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = "ssm:SendCommand"
        Resource = "arn:${data.aws_partition.current.partition}:ssm:${var.aws_region}::document/AWS-RunShellScript"
      },
      {
        Effect   = "Allow"
        Action   = "ssm:SendCommand"
        Resource = "arn:${data.aws_partition.current.partition}:ec2:${var.aws_region}:${data.aws_caller_identity.current.account_id}:instance/*"
        Condition = {
          StringEquals = {
            "ssm:resourceTag/Project"    = local.project_name
            "ssm:resourceTag/Deployment" = var.deployment_id
          }
        }
      },
      {
        Effect   = "Allow"
        Action   = "ec2:TerminateInstances"
        Resource = "arn:${data.aws_partition.current.partition}:ec2:${var.aws_region}:${data.aws_caller_identity.current.account_id}:instance/*"
        Condition = {
          StringEquals = {
            "ec2:ResourceTag/Project"    = local.project_name
            "ec2:ResourceTag/Deployment" = var.deployment_id
          }
        }
      }
    ]
  })
}

resource "aws_iam_role" "safeguard" {
  count       = var.create_compute ? 1 : 0
  name_prefix = "taxi-${substr(var.deployment_id, 0, 24)}-ttl-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "safeguard" {
  count = var.create_compute ? 1 : 0
  name  = "terminate-expired-compute"
  role  = aws_iam_role.safeguard[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "ec2:TerminateInstances"
      Resource = [for node in aws_instance.node : node.arn]
    }]
  })
}
