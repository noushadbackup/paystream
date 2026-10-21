output "cluster_name" {
  value = aws_eks_cluster.this.name
}

output "buckets" {
  value = { for k, b in aws_s3_bucket.this : k => b.bucket }
}

output "service_role_arns" {
  description = "Annotate each service account with its role (deploy/README.md)."
  value       = { for k, r in aws_iam_role.service : k => r.arn }
}

output "postgres_endpoint" {
  value = aws_db_instance.control.address
}

output "postgres_master_secret_arn" {
  value = aws_db_instance.control.master_user_secret[0].secret_arn
}

output "redis_endpoint" {
  value = aws_elasticache_replication_group.online.primary_endpoint_address
}

output "redis_auth_token" {
  value     = random_password.redis.result
  sensitive = true
}
