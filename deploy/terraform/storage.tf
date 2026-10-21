# Three buckets, each with its own lifecycle: the lake (Iceberg data and metadata), Flink checkpoints
# and savepoints, and the model registry. One KMS key encrypts all three.
resource "aws_kms_key" "data" {
  description         = "${local.name} lake, checkpoints and models"
  enable_key_rotation = true
}

resource "aws_kms_alias" "data" {
  name          = "alias/${local.name}-data"
  target_key_id = aws_kms_key.data.key_id
}

locals {
  buckets = {
    lake        = { noncurrent_days = 30 } # Iceberg expires its own snapshots; this catches orphans
    checkpoints = { noncurrent_days = 7 }
    models      = { noncurrent_days = 365 } # every served model stays recoverable for audit
  }
}

resource "aws_s3_bucket" "this" {
  for_each = local.buckets
  bucket   = "${local.name}-${each.key}-${local.account_id}"
}

resource "aws_s3_bucket_versioning" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.data.arn
    }
    bucket_key_enabled = true # one KMS call per object prefix, not per request
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = aws_s3_bucket.this
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  rule { object_ownership = "BucketOwnerEnforced" }
}

resource "aws_s3_bucket_lifecycle_configuration" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  rule {
    id     = "noncurrent"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration { noncurrent_days = local.buckets[each.key].noncurrent_days }
    abort_incomplete_multipart_upload { days_after_initiation = 2 }
  }
  depends_on = [aws_s3_bucket_versioning.this]
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [each.value.arn, "${each.value.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.this]
}
