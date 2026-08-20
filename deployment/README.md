# Blnk MX BEAUTO Kubernetes deployment

This folder is structured after `Lending_Products_Service`'s `deployment/`
folder: a `Dockerfile`, a Jenkins pipeline (`Jenkinsfile` +
`jenkins-build.yaml`) driven by the shared `atlas_shared` library, and
values-only Helm files consumed by the shared Tala microservice chart
(`helm/<app>/global/*.yaml` + `helm/<app>/<country>/*.yaml`).

Because blnk runs two separate processes from one repo (an HTTP API/migration
process and an async worker), it follows the `Windowed_Feature_Service`
precedent of splitting into two app subtrees, each with its own Dockerfile,
image, and Helm value tree:

- `Dockerfile` + `helm/server/` → the `blnk` image (`blnk migrate up && blnk start`)
- `Dockerfile.worker` + `helm/worker/` → the `blnk-worker` image (`blnk workers`)

## What still needs real values before this reaches a cluster

- **Postgres/Redis endpoints.** `helm/{server,worker}/mx/common.yaml` uses
  `${_env}-rds-postgres.data.${_market_hub}.atlas-antelope.com` and
  `${_market_hub}-${_env}-blnk-redis...` placeholders, following the same
  `${_env}`/`${_market_hub}` substitution convention LPS uses. These need to
  be confirmed/replaced once the RDS Postgres instance and ElastiCache Redis
  cluster for blnk are actually provisioned.
- **Secrets.** `blnk-postgres-secret`, `blnk-redis-secret`, and
  `blnk-typesense-secret` need to exist in AWS Secrets Manager (synced via
  ExternalSecrets, matching the `externalSecretsMap`/`externalSecrets`
  pattern) for the `mx`/`beauto` pair before deploy.
- **`helm_chart_version` in `Jenkinsfile`** is a placeholder (copied from
  LPS's pinned version) — confirm the real shared chart version with the
  platform team.
- **Ingress.** `ingress.enabled` is `false` by default. Enable and set a real
  host once the ALB ingress class/DNS name for MX BEAUTO's blnk API is
  decided.
- **Redis AUTH.** `BLNK_REDIS_DNS` currently has no embedded credential. If
  AUTH is enabled on the ElastiCache cluster, prefix the value with
  `$(REDIS_PASSWORD)@` and add a matching `secretEnv` entry (same pattern
  already used for `DB_USER`/`DB_PASSWORD`).

## Dependency deployment strategy

Per the migration story's scope, Postgres and Redis are AWS-managed and
**not** deployed as containers. Typesense and Jaeger, however, have no
first-class support in the shared Tala Helm chart (it only exposes
integration flags for a centrally-run Prometheus and Java/Python
auto-instrumentation) — they're deployed as plain manifests under
`k8s-dependencies/mx-beauto/`, adapted from
`infrastructure/k8s-manifests/{typesense,jaeger}-*.yaml` (this repo's
existing kompose-generated local/dev port), applied via
`kubectl apply -n beauto -f ./deployment/k8s-dependencies/mx-beauto/` in the
Jenkinsfile. Prometheus manifests were **not** carried over — the chart's
built-in `prometheus.enabled`/`path`/`portname` scrape integration is used
instead, avoiding a redundant per-service Prometheus.

**Only `mx-beauto` has dependency manifests right now.** `mx/dev.yaml`,
`mx/qa.yaml`, and `mx/stage.yaml` exist for pipeline completeness (matching
LPS's file set) but don't yet point at their own Typesense/Jaeger instances —
copy `k8s-dependencies/mx-beauto/` into equivalent per-env folders (with a
matching namespace) when those environments are onboarded.
