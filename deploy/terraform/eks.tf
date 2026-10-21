resource "aws_kms_key" "eks" {
  description         = "${local.name} Kubernetes secrets envelope encryption"
  enable_key_rotation = true
}

resource "aws_iam_role" "cluster" {
  name = "${local.name}-eks-cluster"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "eks.amazonaws.com" }, Action = ["sts:AssumeRole", "sts:TagSession"] }]
  })
}

resource "aws_iam_role_policy_attachment" "cluster" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_eks_cluster" "this" {
  name     = local.name
  version  = var.kubernetes_version
  role_arn = aws_iam_role.cluster.arn

  vpc_config {
    subnet_ids              = aws_subnet.private[*].id
    endpoint_private_access = true
    endpoint_public_access  = true
    public_access_cidrs     = var.api_allowed_cidrs
  }

  access_config {
    authentication_mode                         = "API"
    bootstrap_cluster_creator_admin_permissions = false
  }

  encryption_config {
    provider { key_arn = aws_kms_key.eks.arn }
    resources = ["secrets"]
  }

  enabled_cluster_log_types = ["api", "audit", "authenticator"]
  depends_on                = [aws_iam_role_policy_attachment.cluster]
}

resource "aws_eks_access_entry" "admin" {
  for_each      = toset(var.cluster_admin_role_arns)
  cluster_name  = aws_eks_cluster.this.name
  principal_arn = each.value
}

resource "aws_eks_access_policy_association" "admin" {
  for_each      = toset(var.cluster_admin_role_arns)
  cluster_name  = aws_eks_cluster.this.name
  principal_arn = each.value
  policy_arn    = "arn:${local.partition}:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
  access_scope { type = "cluster" }
  depends_on = [aws_eks_access_entry.admin]
}

# ------------------------------------------------------------------------------------- node groups

resource "aws_iam_role" "node" {
  name = "${local.name}-eks-node"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "ec2.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "node" {
  for_each = toset(["AmazonEKSWorkerNodePolicy", "AmazonEKS_CNI_Policy", "AmazonEC2ContainerRegistryReadOnly"])
  role     = aws_iam_role.node.name
  # pods get AWS access through their own service account roles (iam.tf), never the node's
  policy_arn = "arn:${local.partition}:iam::aws:policy/${each.value}"
}

resource "aws_launch_template" "node" {
  for_each = var.node_groups
  name     = "${local.name}-${each.key}"
  # IMDSv2 only, one hop: pods cannot reach the node's instance credentials
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }
  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size = each.value.disk_gb
      volume_type = "gp3"
      encrypted   = true
    }
  }
  tag_specifications {
    resource_type = "instance"
    tags          = { Name = "${local.name}-${each.key}" }
  }
}

resource "aws_eks_node_group" "this" {
  for_each        = var.node_groups
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = each.key
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = aws_subnet.private[*].id
  instance_types  = each.value.instance_types
  ami_type        = "AL2023_x86_64_STANDARD"
  labels          = { workload = each.key }

  scaling_config {
    min_size     = each.value.min_size
    max_size     = each.value.max_size
    desired_size = each.value.desired_size
  }

  launch_template {
    id      = aws_launch_template.node[each.key].id
    version = aws_launch_template.node[each.key].latest_version
  }

  dynamic "taint" {
    for_each = each.value.taint == null ? [] : [each.value.taint]
    content {
      key    = "dedicated"
      value  = taint.value
      effect = "NO_SCHEDULE"
    }
  }

  update_config { max_unavailable = 1 }
  # the cluster autoscaler owns the size after creation
  lifecycle { ignore_changes = [scaling_config[0].desired_size] }
  depends_on = [aws_iam_role_policy_attachment.node]
}

# ------------------------------------------------------------------------------- IRSA and add-ons

data "tls_certificate" "oidc" {
  url = aws_eks_cluster.this.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "eks" {
  url             = aws_eks_cluster.this.identity[0].oidc[0].issuer
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.oidc.certificates[0].sha1_fingerprint]
}

resource "aws_eks_addon" "this" {
  for_each                    = toset(["vpc-cni", "coredns", "kube-proxy", "aws-ebs-csi-driver"])
  cluster_name                = aws_eks_cluster.this.name
  addon_name                  = each.value
  service_account_role_arn    = each.value == "aws-ebs-csi-driver" ? aws_iam_role.service["ebs-csi"].arn : null
  resolve_conflicts_on_update = "OVERWRITE"
  depends_on                  = [aws_eks_node_group.this]
}
