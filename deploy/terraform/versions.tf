terraform {
  required_version = ">= 1.10" # S3 native state locking
  required_providers {
    aws    = { source = "hashicorp/aws", version = "~> 5.100" }
    tls    = { source = "hashicorp/tls", version = "~> 4.0" }
    random = { source = "hashicorp/random", version = "~> 3.6" }
  }
  # State lives in S3 with locking; the bucket is per account and given at init:
  #   terraform init -backend-config=environments/prod.backend.hcl
  backend "s3" {}
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { project = "paystream", environment = var.environment, managed_by = "terraform" }
  }
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
data "aws_availability_zones" "available" { state = "available" }

locals {
  name       = "${var.name}-${var.environment}"
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition
  azs        = slice(data.aws_availability_zones.available.names, 0, 3)
}
