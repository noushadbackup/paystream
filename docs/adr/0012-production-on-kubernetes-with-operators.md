# 12. Production runs on Kubernetes with operators; only the cloud layer is AWS-specific

**Status:** accepted

**Context.** The compose stack proves the design on one machine; a payments company needs it on
infrastructure that survives a zone failure, scales with salary-day and Eid traffic, upgrades
stateful jobs without losing state, and passes a security review: encrypted links, a separate
identity per service, least privilege, no shared admin passwords. Some providers must also run in
their own data centre for data residency, so the deployment cannot depend on one cloud's streaming
services.

**Decision.**
* **Kubernetes with operators** for the stateful parts. Strimzi runs Kafka in KRaft mode (no
  ZooKeeper), three brokers spread over zones, rack-aware replica placement and follower fetching,
  `min.insync.replicas=2`, topics declared as `KafkaTopic` resources, Cruise Control for rebalancing.
  The Flink Kubernetes Operator runs both jobs as `FlinkDeployment`s with `upgradeMode: savepoint`
  (every deploy stops with a savepoint and restores from it), Kubernetes HA with a standby
  JobManager, and the operator's autoscaler resizing vertices from busy time and Kafka lag.
* **Rejected: MSK plus Managed Service for Apache Flink.** Less to operate, but AWS-only, and
  managed Flink gives less control over savepoint upgrades and state backend settings. The manifests
  here run on any conformant Kubernetes (EKS, GKE, OpenShift on premises); only `deploy/terraform`
  is AWS-specific.
* **Every Kafka client authenticates.** TLS listener with SCRAM-SHA-512, a `KafkaUser` per service
  with ACLs for exactly the topics and groups it uses (the decision service can only write
  decisions; nothing but the case service writes labels). Clients read their settings from
  `KAFKA_CFG_*` environment variables, so the same images run unauthenticated in compose and over
  SASL_SSL in the cluster without code changes; `ClientConfig` (Java) and `kafka_env` (Python)
  implement the convention and are unit-tested.
* **Cloud identity per pod** (IRSA): one IAM role per service account, scoped to the buckets it
  needs; the decision service's model sync can read the registry but never write it. Nodes allow
  IMDSv2 with one hop only, so pods cannot borrow node credentials.
* **Managed data stores where state is small and critical.** Postgres (rules, cases, Iceberg
  catalog) on RDS Multi-AZ with forced TLS and a Secrets Manager-managed master password; Redis on
  ElastiCache with TLS, an auth token and automatic failover. Redis runs with cluster mode off: the
  decision service reads sender, receiver and device keys in one pipeline, which sharding would
  split; one r7g.large holds tens of millions of feature buckets, and reads scale with replicas.
* **Network.** Default-deny ingress in the namespace; the decision API accepts traffic only from
  the payment gateway's namespace and Prometheus; analyst UIs only through the ingress controller.
* **Dedicated node groups** for Kafka and Flink, tainted, so a busy job cannot starve brokers and
  the services' autoscaling cannot evict either.

**Consequences.** The platform team operates Strimzi and the Flink operator (upgrades, CRD
versions). Placeholders (`REGISTRY`, `LAKE_BUCKET`, ...) in the Flink manifests are filled from
Terraform outputs at deploy time; secrets are created from Terraform outputs or, better, synced by
External Secrets from Secrets Manager. CI lints the chart, checks every rendered object and custom
resource against its schema, and validates the Terraform; none of this has yet been applied to a
real AWS account.
