# Deploying PayStream

The design is in [ADR 12](../docs/adr/0012-production-on-kubernetes-with-operators.md). Three layers:

| Layer | What | Where |
| --- | --- | --- |
| Cloud | VPC over three zones, EKS with Kafka/Flink/services node groups, S3 (lake, checkpoints, models) under KMS, RDS Postgres, ElastiCache Redis, an IAM role per service account | [`terraform/`](terraform) |
| Streaming | Strimzi Kafka (KRaft, TLS + SCRAM, per-service ACLs), topics, Debezium on Kafka Connect; both Flink jobs under the Flink operator | [`k8s/kafka`](k8s/kafka), [`k8s/flink`](k8s/flink) |
| Services | decision, rules and case services, Ops Console, Schema Registry, Iceberg REST catalog; network policies | [`helm/paystream`](helm/paystream), Trino from its own chart with [`helm/trino-values.yaml`](helm/trino-values.yaml) |

`make deploy-check` runs what CI runs: `helm lint`, every rendered object and custom resource
checked against its schema with kubeconform, and `terraform fmt` and `validate`.

## Order of a first install

1. **Cloud.** Create a state bucket, then
   ```bash
   cd deploy/terraform
   terraform init -backend-config=environments/prod.backend.hcl
   terraform apply -var-file=environments/prod.tfvars
   aws eks update-kubeconfig --name "$(terraform output -raw cluster_name)"
   ```
2. **Cluster add-ons** (their own charts, pinned): cert-manager, Strimzi 0.44+ watching the
   `paystream` namespace, the Flink Kubernetes Operator 1.10+ with
   `jobServiceAccount.annotations."eks\.amazonaws\.com/role-arn"` set to the `flink` role from
   `terraform output service_role_arns`, cluster autoscaler or Karpenter, kube-prometheus-stack,
   ingress-nginx.
3. **Kafka.**
   ```bash
   kubectl create namespace paystream
   kubectl apply -f deploy/k8s/storageclass.yaml
   kubectl apply -f deploy/k8s/kafka/kafka.yaml -f deploy/k8s/kafka/topics.yaml -f deploy/k8s/kafka/users.yaml
   kubectl -n paystream wait kafka/paystream --for=condition=Ready --timeout=20m
   ```
   Strimzi writes each user's password and JAAS line into a secret named after the user, and the
   cluster CA into `paystream-cluster-ca-cert`; the chart and the Flink jobs mount those.
4. **Databases and secrets.** With the master password from `postgres_master_secret_arn`, create
   the `rules`, `cases` and `iceberg_catalog` databases, each with its own role. Then the secrets the
   chart reads (in production, sync these from Secrets Manager with External Secrets instead):
   ```bash
   kubectl -n paystream create secret generic online-store --from-literal=auth-token="$(terraform output -raw redis_auth_token)"
   kubectl -n paystream create secret generic rules-db --from-literal=url='postgresql+psycopg://rules:...@HOST/rules?sslmode=require'
   kubectl -n paystream create secret generic cases-db --from-literal=url='postgresql+psycopg://cases:...@HOST/cases?sslmode=require'
   kubectl -n paystream create secret generic iceberg-catalog-db --from-literal=uri='jdbc:postgresql://HOST:5432/iceberg_catalog?sslmode=require' --from-literal=username=iceberg --from-literal=password=...
   kubectl -n paystream create secret generic rules-tokens --from-literal=tokens='...'   # token=name:role, from your identity provider
   kubectl -n paystream create secret generic cases-tokens --from-literal=tokens='...'
   kubectl -n paystream create secret generic ops-console --from-literal=cases-token='...'
   ```
5. **Services.**
   ```bash
   helm upgrade --install paystream deploy/helm/paystream -n paystream \
     --set image.registry=ACCOUNT.dkr.ecr.ap-south-1.amazonaws.com/paystream --set image.tag=$VERSION \
     --set redis.host="$(terraform output -raw redis_endpoint)" \
     --set icebergRest.warehouse=s3://LAKE_BUCKET/ --set icebergRest.region=ap-south-1 \
     --set decisionService.models.s3Uri=s3://MODELS_BUCKET/registry \
     --set serviceAccounts.modelsReader=ROLE_ARN --set serviceAccounts.lakeCatalog=ROLE_ARN
   helm upgrade --install trino trino/trino -n paystream -f deploy/helm/trino-values.yaml
   ```
6. **CDC.** The ledger is the core banking database, not created here. It needs
   `rds.logical_replication=1` (or `wal_level=logical`), a replication role and a publication for
   the ledger tables; put its credentials in the `ledger-db` secret, then
   `kubectl apply -f deploy/k8s/kafka/connect.yaml`.
7. **Flink jobs.** On a first deployment, `device-history-bootstrap.yaml` runs first: it writes 90
   days of account device history from the lakehouse into the savepoint the payments job starts
   from (`initialSavepointPath`), so takeover detection is warm on day one
   ([ADR 13](../docs/adr/0013-warm-start-from-the-lakehouse.md)). Fill the placeholders from
   Terraform outputs and apply, the bootstrap first, waiting for it to finish:
   ```bash
   export REGISTRY=... VERSION=... CHECKPOINT_BUCKET=... LAKE_BUCKET=... AWS_REGION=ap-south-1 REDIS_ENDPOINT=...
   render() {
     sed -e "s|REGISTRY|$REGISTRY|g; s|VERSION|$VERSION|g; s|CHECKPOINT_BUCKET|$CHECKPOINT_BUCKET|g" \
         -e "s|LAKE_BUCKET|$LAKE_BUCKET|g; s|AWS_REGION|$AWS_REGION|g; s|REDIS_ENDPOINT|$REDIS_ENDPOINT|g" "$1"
   }
   render deploy/k8s/flink/device-history-bootstrap.yaml | kubectl apply -f -
   kubectl -n paystream wait flinkdeployment/device-history-bootstrap \
     --for=jsonpath='{.status.jobStatus.state}'=FINISHED --timeout=2h
   kubectl -n paystream delete flinkdeployment/device-history-bootstrap
   for f in deploy/k8s/flink/{payments-job,ledger-cdc-job}.yaml; do render "$f" | kubectl apply -f -; done
   ```
   A later image change is just a new `spec.image`: the operator takes a savepoint, stops the job,
   and starts the new version from it.

## Day two

* **Upgrading a job:** change the image, apply; watch `kubectl get flinkdeployment`. If the new
  version fails, set the previous image back: it restores from the last savepoint.
* **Scaling Kafka:** add brokers to the `KafkaNodePool`, then a `KafkaRebalance` (Cruise Control)
  moves partitions onto them. Partition counts only grow, and the Flink job's max parallelism (480)
  is a multiple of the payments topic's 48 partitions.
* **Rotating a service's Kafka password:** delete its `KafkaUser` secret; Strimzi issues a new one
  and the pods pick it up on restart.
* **Models:** the training DAG writes to the models bucket with the `ml-training` role; the
  decision service's sidecar syncs it every 30 s and the service re-reads its pointers, so a
  promotion or rollback needs no deploy.

## Not done yet

This has been validated against schemas and `terraform validate`, not applied to an AWS account.
Images are built from the Dockerfiles in each service directory but no registry push pipeline is
included.
