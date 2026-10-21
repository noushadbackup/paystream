variable "name" {
  type    = string
  default = "paystream"
}

variable "environment" {
  type = string
}

variable "region" {
  type        = string
  description = "ap-south-1 (Mumbai) is the closest region to Dhaka."
  default     = "ap-south-1"
}

variable "vpc_cidr" {
  type    = string
  default = "10.40.0.0/16"
}

variable "single_nat_gateway" {
  type        = bool
  description = "One NAT gateway instead of one per zone: cheaper, but a zone outage cuts egress for all."
  default     = false
}

variable "kubernetes_version" {
  type    = string
  default = "1.31"
}

variable "api_allowed_cidrs" {
  type        = list(string)
  description = "Who may reach the public Kubernetes API endpoint (VPN and CI egress)."
}

variable "cluster_admin_role_arns" {
  type        = list(string)
  description = "IAM roles granted cluster-admin through EKS access entries (platform team, CI deployer)."
  default     = []
}

# Node groups. Kafka and Flink get their own nodes so a noisy job cannot starve the brokers.
variable "node_groups" {
  type = map(object({
    instance_types = list(string)
    min_size       = number
    max_size       = number
    desired_size   = number
    disk_gb        = number
    taint          = optional(string)
  }))
  default = {
    system   = { instance_types = ["m6i.large"], min_size = 3, max_size = 6, desired_size = 3, disk_gb = 50 }
    services = { instance_types = ["c6i.xlarge"], min_size = 3, max_size = 30, desired_size = 3, disk_gb = 50 }
    kafka    = { instance_types = ["r6i.xlarge"], min_size = 3, max_size = 6, desired_size = 3, disk_gb = 50, taint = "kafka" }
    flink    = { instance_types = ["r6i.2xlarge"], min_size = 2, max_size = 20, desired_size = 3, disk_gb = 200, taint = "flink" }
  }
}

variable "postgres_instance_class" {
  type    = string
  default = "db.r6g.large"
}

variable "redis_node_type" {
  type    = string
  default = "cache.r7g.large"
}

variable "redis_replicas" {
  type        = number
  description = "Read replicas per shard; with automatic failover one of them is promoted when the primary fails."
  default     = 2
}

variable "airflow_service_account" {
  type        = string
  description = "namespace:serviceaccount of the Airflow workers that train and promote models."
  default     = "airflow:airflow-worker"
}
