# env_monitor_streaming

Real-time streaming pipeline for the Smart City Wildfire & Environmental Monitoring system. Consumes live sensor data from [env_monitor_api](https://github.com/LoganLRB/env_monitor_api) via SSE, publishes to Kafka, and runs two parallel consumer groups: one for severity-escalation alerts and one for writing live state to Redis.

## Architecture

```
env_monitor_api
  GET /v1/sensors/stream  (SSE, named events)
        |
        v  SSE consumer (httpx streaming)
  Kafka topic: sensor.readings
        |
        +-------------------------------+
        |                               |
        v  alert-consumer               v  state-consumer
  Alert engine                    Redis state writer
  - severity escalation           - sensor:current:{id}   (10-min TTL)
  - local: stdout                 - sensor:history:{id}   (15-min sorted set)
  - prod: SNS publish             - sensor:severity:{id}  (dedup key)
                                  - pub/sub: sensor.live  (for dashboard SSE)
```

Alert severity levels: `LOW` < `MODERATE` < `HIGH` < `CRITICAL`. An SNS alert fires only when a sensor's new reading exceeds its stored severity. De-escalation updates the stored severity without firing.

## Infrastructure (Terraform)

```bash
# One-time: create the Terraform state bucket and DynamoDB lock table manually
aws s3 mb s3://env-monitor-terraform-state
aws dynamodb create-table \
  --table-name env-monitor-terraform-locks \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

cd terraform
terraform init
terraform apply -var="api_base_url=https://your-api-url"
```

Resources created:

| Resource | Purpose |
|---|---|
| MSK cluster (`kafka.t3.small`, 2 brokers) | Managed Kafka for `sensor.readings` topic |
| ElastiCache Redis (`cache.t3.micro`) | Live sensor state + pub/sub for dashboard |
| ECS Fargate service | Runs the streaming container |
| ECR repository | Docker image storage |
| SNS topic `env-monitor-alerts` | Wildfire severity alert notifications |
| VPC + private subnets + NAT GW | Network isolation for ECS, MSK, Redis |
| IAM roles | Least-privilege task role (SSM read, SNS publish) |
| SSM parameter `/env-monitor-shared/REDIS_URL` | Shared with `env_monitor_dashboard` |

## Local Development (Docker Compose)

**Prerequisites:** Docker Desktop and [env_monitor_api](https://github.com/LoganLRB/env_monitor_api) running on port 8000.

```bash
cp .env.example .env
docker compose up --build -d
```

The stack starts three containers: `apache/kafka:latest` (KRaft mode, port 9094 external), `redis:7-alpine` (port 6379), and the streaming service itself. Kafka and Redis health checks must pass before the streaming container starts.

Verify data is flowing:

```bash
# Check streaming service logs
docker compose logs streaming -f

# Inspect Redis keys written by the state consumer
docker exec <redis-container> redis-cli keys "sensor:*"

# Spot-check a current reading
docker exec <redis-container> redis-cli get "sensor:current:SNS-001"
```

## Running Standalone (without Docker)

Useful for development iteration. Requires Kafka and Redis already running (e.g. from `docker compose up kafka redis -d`).

```bash
cp .env.example .env
# Edit .env: set KAFKA_BOOTSTRAP_SERVERS=localhost:9094 and REDIS_URL=redis://localhost:6379
python -m venv env_monitor_streaming
source env_monitor_streaming/bin/activate     # Windows: env_monitor_streaming\Scripts\activate
pip install -r requirements.txt
python main.py
```

## Tests

```bash
python -m venv env_monitor_streaming
source env_monitor_streaming/bin/activate
pip install -r requirements.txt
python -m unittest discover tests -v
```

Tests use `fakeredis` for Redis mocking and `moto[sns]` for SNS. No Kafka broker or AWS credentials required.

## Configuration

All settings are managed via `streaming/config.py` (pydantic-settings). In production, values are loaded from AWS SSM Parameter Store at `/env-monitor-streaming/`; in local dev they are read from `.env`.

| Variable | Default | Description |
|---|---|---|
| `ENVIRONMENT` | `local` | Set to anything other than `local` to enable SSM loading |
| `API_BASE_URL` | `http://localhost:8000` | Base URL of env_monitor_api (`host.docker.internal:8000` in Docker) |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address (`kafka:9092` in Docker) |
| `KAFKA_TOPIC` | `sensor.readings` | Topic name for all sensor readings |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL (`redis://redis:6379` in Docker) |
| `SNS_ALERT_TOPIC_ARN` | `` | SNS topic ARN for alerts; leave blank in local dev |

## CI/CD (GitHub Actions)

| Workflow | Trigger | What it does |
|---|---|---|
| `plan.yml` | PR to `main` | `terraform init` + `plan`; posts collapsible output as a PR comment; blocks merge if plan fails |
| `deploy.yml` | Manual (`workflow_dispatch`) | Runs tests, `terraform apply`, builds and pushes Docker image to ECR, forces ECS redeployment |

**Required GitHub Actions variables** (Settings > Secrets and variables > Actions > Variables):

- `AWS_ROLE_DEV`: OIDC role ARN for the dev environment
- `API_BASE_URL`: URL of the running env_monitor_api

## Project Structure

```
env_monitor_streaming/
├── streaming/
│   ├── config.py          # Pydantic Settings + SSM loader
│   ├── models.py          # SensorReading (matches env_monitor_api exactly)
│   ├── consumer.py        # SSE consumer -> Kafka producer (with exponential backoff reconnect)
│   ├── alert.py           # alert-consumer: severity escalation engine + SNS publish
│   └── state.py           # state-consumer: Redis current/history/severity writes + pub/sub
├── main.py                # Entrypoint: starts producer, two consumers, Redis; asyncio.gather
├── terraform/
│   ├── providers.tf / backend.tf / variables.tf / outputs.tf
│   ├── vpc.tf             # VPC, private subnets, NAT GW
│   ├── msk.tf             # MSK Kafka cluster (kafka.t3.small, 2 brokers, KRaft)
│   ├── elasticache.tf     # ElastiCache Redis (cache.t3.micro)
│   ├── ecr.tf             # ECR repository
│   ├── ecs.tf             # ECS cluster + Fargate task + service
│   ├── iam.tf             # ECS task role (SSM read, SNS publish)
│   ├── sns.tf             # SNS topic + optional email subscription
│   └── ssm.tf             # SSM parameters + shared /env-monitor-shared/REDIS_URL
├── .github/workflows/
│   ├── plan.yml           # Terraform plan on PR, output posted as comment
│   └── deploy.yml         # Manual dispatch: test + terraform apply + ECR push + ECS redeploy
├── tests/
│   ├── test_alert.py      # Alert severity escalation logic (fakeredis + moto[sns])
│   └── test_state.py      # Redis state writes and pub/sub (fakeredis)
├── requirements.txt
├── Dockerfile
└── docker-compose.yml     # Local dev: apache/kafka + redis + streaming service
```
