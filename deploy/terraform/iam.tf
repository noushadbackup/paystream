# One IAM role per Kubernetes service account (IRSA), each limited to the buckets and prefixes it
# needs. Nothing in the cluster uses node credentials.
locals {
  oidc   = replace(aws_eks_cluster.this.identity[0].oidc[0].issuer, "https://", "")
  bucket = { for k, b in aws_s3_bucket.this : k => b.arn }

  service_roles = {
    # Flink writes Iceberg data files and its own checkpoints, savepoints and HA metadata
    flink = {
      subject = "paystream:flink"
      write   = [local.bucket.lake, local.bucket.checkpoints]
      read    = []
    }
    # the REST catalog writes table metadata on commit
    iceberg-rest = {
      subject = "paystream:iceberg-rest"
      write   = [local.bucket.lake]
      read    = []
    }
    trino = {
      subject = "paystream:trino"
      write   = []
      read    = [local.bucket.lake]
    }
    # the decision service's sidecar syncs the registry; it can never change a model
    decision-service = {
      subject = "paystream:decision-service"
      write   = []
      read    = [local.bucket.models]
    }
    # training reads the decision log and labels, writes candidates and moves the pointers
    ml-training = {
      subject = var.airflow_service_account
      write   = [local.bucket.models]
      read    = [local.bucket.lake]
    }
    ebs-csi = {
      subject = "kube-system:ebs-csi-controller-sa"
      write   = []
      read    = []
    }
  }
}

resource "aws_iam_role" "service" {
  for_each = local.service_roles
  name     = "${local.name}-${each.key}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = aws_iam_openid_connect_provider.eks.arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${local.oidc}:sub" = "system:serviceaccount:${each.value.subject}"
          "${local.oidc}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "buckets" {
  for_each = { for k, v in local.service_roles : k => v if length(v.read) + length(v.write) > 0 }
  name     = "buckets"
  role     = aws_iam_role.service[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [{
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = concat(each.value.read, each.value.write)
      }],
      length(each.value.read) == 0 ? [] : [{
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = [for b in each.value.read : "${b}/*"]
      }],
      length(each.value.write) == 0 ? [] : [{
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource = [for b in each.value.write : "${b}/*"]
      }],
      [{
        Effect   = "Allow"
        Action   = length(each.value.write) == 0 ? ["kms:Decrypt"] : ["kms:Decrypt", "kms:GenerateDataKey"]
        Resource = [aws_kms_key.data.arn]
      }],
    )
  })
}

resource "aws_iam_role_policy_attachment" "ebs_csi" {
  role       = aws_iam_role.service["ebs-csi"].name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}
