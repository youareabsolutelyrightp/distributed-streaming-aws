variable "aws_region" {
  description = "AWS Region for this temporary deployment."
  type        = string

  validation {
    condition     = can(regex("^[a-z]{2}(-[a-z]+)+-[0-9]+$", var.aws_region))
    error_message = "aws_region must be a valid AWS Region name."
  }
}

variable "deployment_id" {
  description = "Unique identifier for one temporary deployment."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{2,31}$", var.deployment_id))
    error_message = "deployment_id must contain 3 to 32 lowercase letters, numbers, or hyphens."
  }
}

variable "owner_tag" {
  description = "Owner value for cost and cleanup tags."
  type        = string

  validation {
    condition     = length(trimspace(var.owner_tag)) > 0
    error_message = "owner_tag must not be empty."
  }
}

variable "create_compute" {
  description = "Create compute after the release exists in the artifact bucket."
  type        = bool
  default     = false
}

variable "release_object_key" {
  description = "S3 object key for the immutable release archive."
  type        = string
  default     = ""
}

variable "release_sha256" {
  description = "SHA-256 digest for the immutable release archive."
  type        = string
  default     = ""

  validation {
    condition     = var.release_sha256 == "" || can(regex("^[0-9a-f]{64}$", var.release_sha256))
    error_message = "release_sha256 must be empty or a lowercase SHA-256 digest."
  }
}

variable "deployment_expires_at" {
  description = "UTC time when the safeguard terminates the instances."
  type        = string
  default     = ""

  validation {
    condition = (
      var.deployment_expires_at == "" ||
      can(formatdate("YYYY-MM-DD'T'hh:mm:ss'Z'", var.deployment_expires_at))
    )
    error_message = "deployment_expires_at must be an RFC 3339 timestamp."
  }
}

variable "ttl_hours" {
  description = "Maximum local shutdown delay."
  type        = number
  default     = 6

  validation {
    condition     = var.ttl_hours >= 1 && var.ttl_hours <= 6
    error_message = "ttl_hours must be from 1 through 6."
  }
}

variable "force_destroy_artifacts" {
  description = "Permit Terraform to delete temporary artifacts during destroy."
  type        = bool
  default     = true
}
