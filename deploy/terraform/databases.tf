# Postgres for the control plane (rules, cases, the Iceberg catalog) and Redis for the online store.
# The ledger database Debezium reads is the core banking system's, not created here.

resource "aws_security_group" "data" {
  name        = "${local.name}-data"
  description = "Postgres and Redis, reachable from the cluster only"
  vpc_id      = aws_vpc.this.id
}

resource "aws_vpc_security_group_ingress_rule" "data" {
  for_each                     = { postgres = 5432, redis = 6379 }
  security_group_id            = aws_security_group.data.id
  referenced_security_group_id = aws_eks_cluster.this.vpc_config[0].cluster_security_group_id
  ip_protocol                  = "tcp"
  from_port                    = each.value
  to_port                      = each.value
  description                  = each.key
}

# ------------------------------------------------------------------------------------------ Postgres

resource "aws_db_subnet_group" "this" {
  name       = local.name
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_db_parameter_group" "this" {
  name   = "${local.name}-pg16"
  family = "postgres16"
  parameter {
    name  = "rds.force_ssl"
    value = "1"
  }
  parameter {
    name  = "log_min_duration_statement"
    value = "500"
  }
}

resource "aws_db_instance" "control" {
  identifier                   = "${local.name}-control"
  engine                       = "postgres"
  engine_version               = "16.4"
  instance_class               = var.postgres_instance_class
  allocated_storage            = 100
  max_allocated_storage        = 1000
  storage_type                 = "gp3"
  storage_encrypted            = true
  kms_key_id                   = aws_kms_key.data.arn
  db_name                      = "paystream"
  username                     = "paystream_admin"
  manage_master_user_password  = true # generated and rotated in Secrets Manager, never in state
  multi_az                     = true
  db_subnet_group_name         = aws_db_subnet_group.this.name
  vpc_security_group_ids       = [aws_security_group.data.id]
  parameter_group_name         = aws_db_parameter_group.this.name
  backup_retention_period      = 14
  deletion_protection          = true
  skip_final_snapshot          = false
  final_snapshot_identifier    = "${local.name}-control-final"
  performance_insights_enabled = true
  auto_minor_version_upgrade   = true
  copy_tags_to_snapshot        = true
}

# --------------------------------------------------------------------------------------------- Redis

# The auth token goes to the cluster as a Kubernetes secret (see deploy/README.md); it is in
# Terraform state, so state is encrypted and access to it is restricted.
resource "random_password" "redis" {
  length  = 64
  special = false
}

resource "aws_elasticache_subnet_group" "this" {
  name       = local.name
  subnet_ids = aws_subnet.private[*].id
}

# Cluster mode off: the decision service reads a payment's sender, receiver and device keys in one
# pipeline, which cluster mode would split across shards. One primary, replicas in other zones.
resource "aws_elasticache_replication_group" "online" {
  replication_group_id       = "${local.name}-online"
  description                = "PayStream online feature store and flags"
  engine                     = "redis"
  engine_version             = "7.1"
  node_type                  = var.redis_node_type
  num_cache_clusters         = 1 + var.redis_replicas
  automatic_failover_enabled = true
  multi_az_enabled           = true
  subnet_group_name          = aws_elasticache_subnet_group.this.name
  security_group_ids         = [aws_security_group.data.id]
  transit_encryption_enabled = true
  at_rest_encryption_enabled = true
  kms_key_id                 = aws_kms_key.data.arn
  auth_token                 = random_password.redis.result
  snapshot_retention_limit   = 3
  maintenance_window         = "sun:20:00-sun:21:00" # 02:00 Dhaka, the quietest hour
  snapshot_window            = "18:00-19:00"
}
