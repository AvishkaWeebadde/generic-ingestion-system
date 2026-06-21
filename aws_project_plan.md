# Blueprint 1: High-Throughput Webhook Ingestion & Processing Engine
## Architectural Engineering & Delivery Roadmap (Java-first, Go migration)

This document is your complete engineering blueprint, execution playbook, and operations guide for building a resilient, asynchronous, event-driven architecture on AWS. It is structured for an engineer with strong software fundamentals (4+ yrs Java) looking to master production-grade cloud infrastructure, distributed observability, and automated continuous delivery.

**Language strategy:** Build the entire system in **Java 17** first (Phases 1–4) so the *cloud architecture* — not language friction — is the thing you're learning. Once the architecture is second nature, **port the worker to Go** (Phase 5) as a focused concurrency exercise. The architecture is language-agnostic; only the worker container changes.

> **Note for the AI assistant (Claude Code):** Act as an expert systems/cloud architect. Identify the minimal steps per phase and advise. Do **not** write application code the developer has not written yet — review their code, suggest better idioms, and unblock them. Editing *this plan document* and infra/config scaffolding guidance is in scope.

---

## 1. Architectural System Overview

The core objective is to simulate an enterprise-grade ingestion system (think Stripe/Twilio) capable of receiving a heavy volume of HTTP payloads, decoupling ingestion from processing via an asynchronous queue, running worker pools with independent scaling, and emitting granular distributed traces.

### System Topology

```
[External Webhook Clients]
        │
        ▼ (HTTPS POST)
┌────────────────────────────────────────────────────────┐
│ Amazon API Gateway (HTTP API)                          │
└────────────────────────────────────────────────────────┘
        │
        ▼ (Direct Service Integration via IAM Role — no Lambda)
┌────────────────────────────────────────────────────────┐
│ Amazon SQS (Inbound Ingestion Queue)                   │
└────────────────────────────────────────────────────────┘
        │
        ▼ (Long-Polling via HTTP/1.1)
┌────────────────────────────────────────────────────────┐
│ Amazon ECS Cluster                                     │
│  └─ Service (Desired Count: 1)                         │
│      └─ Task (Fargate, 0.25 vCPU, 0.5 GB)              │
│          ├─ Container 1: Java Worker App               │
│          └─ Container 2: AWS OTel Collector Sidecar    │
└────────────────────────────────────────────────────────┘
        │                                  │
        ▼ (Write)                          ▼ (gRPC / OTLP)
┌───────────────────────────┐    ┌───────────────────────┐
│ Amazon DynamoDB           │    │ Amazon CloudWatch     │
│ (Key-Value Store)         │    │ (Logs, Metrics, X-Ray)│
└───────────────────────────┘    └───────────────────────┘
```

### Components & Design Rationale
* **Amazon API Gateway (HTTP API):** Low latency, native direct integrations. Rather than invoking a Lambda to push to SQS (extra compute hop, latency, cost), API Gateway acts as a **direct proxy to SQS**.
* **Amazon SQS (Standard Queue):** Elastic buffer absorbing traffic spikes so downstream isn't overwhelmed. Standard queues guarantee at-least-once delivery and near-infinite horizontal throughput.
* **Amazon ECS on AWS Fargate:** Serverless container compute. Removes EC2 host management while letting a continuous background process (the worker) poll SQS efficiently.
* **AWS Distro for OpenTelemetry (ADOT) Sidecar:** Runs alongside the worker container in the same Fargate task. Intercepts exported OTel spans/metrics locally, then ships them to CloudWatch and X-Ray.
* **Amazon DynamoDB:** Fully managed NoSQL, single-digit-ms latency. Holds the final processing status of each webhook.

### Why Java first (then Go)
* **OTel auto-instrumentation is Java's superpower.** The OpenTelemetry Java agent auto-traces AWS SDK v2 (SQS, DynamoDB) and HTTP with near-zero manual span code — collapsing most of Phase 3 into "attach one `-javaagent` jar."
* **AWS SDK v2 for Java is first-class** and well-documented for the SQS long-poll loop and DynamoDB writes.
* **Lower cognitive load.** The novel material here is AWS + tracing + IaC + CI/CD. Learn that on familiar Java ground; revisit Go concurrency (goroutines/channels) in Phase 5 once you already understand *what* the code must do.

---

## 2. Phase-by-Phase Execution Guide

Build incrementally. **Do not provision the whole pipeline at once.** Complete and test each phase before moving on. Each sub-task lists *what you build* and a *done-test*.

---

### Phase 1: Local Application Core & Containerization (no AWS account touched)

Establish a fully observable application locally before any cloud work.

#### Task 1.1 — Project shape & dependencies
1. Package root is `org.phoenix.ingestion` (already scaffolded).
2. Add to `pom.xml` (you write these):
   * **AWS SDK v2 BOM** + `sqs` + `dynamodb` (hold the actual client calls until Phase 2 — just the deps).
   * **Logging:** `ch.qos.logback:logback-classic` + `net.logstash.logback:logstash-logback-encoder` (JSON logs).
   * **JSON:** `com.fasterxml.jackson.core:jackson-databind`.
   * **Build:** `maven-shade-plugin` to produce one executable fat jar.
   * *Defer* all OpenTelemetry deps until Phase 3.
* **Done-test:** `mvn -q package` produces a single runnable jar; `java -jar target/...jar` runs.

#### Task 1.2 — Model the inbound event
1. Create a Java `record` for the payload — records are ideal for immutable DTOs:
   ```json
   {
     "event_id": "evt_987654321",
     "event_type": "user.payment_processed",
     "timestamp": "2026-06-21T07:00:00Z",
     "payload": { "amount": 4900, "currency": "usd", "user_id": "usr_12345" }
   }
   ```
2. Map it with Jackson (`@JsonProperty` for snake_case → camelCase).
* **Done-test:** A small `main` deserializes the sample JSON string into your record and prints a field.

#### Task 1.3 — Core processing engine
1. A processor method that: validates required fields → checks `amount > 0` → applies a deterministic simulated delay (`Thread.sleep` between 50–150 ms).
2. On invalid input, throw/return a typed result you can log distinctly (this becomes your error-span path in Phase 3).
* **Done-test:** Valid event → "processed" with a measured `duration_ms`; invalid event → a clean rejection, no crash.

#### Task 1.4 — The consumer loop (mocked source)
1. Write a loop that simulates an SQS consumer but reads JSON from **a local file or stdin** for now — do not touch AWS yet.
2. Structure it so the message *source* is an interface (e.g. `MessageSource`) with a local impl now and a real SQS impl in Phase 2. This keeps Phase 2 a drop-in swap.
3. (Optional, foreshadows worker pools) process messages on an `ExecutorService` fixed thread pool.
* **Done-test:** Loop ingests a batch of sample events end-to-end and exits cleanly.

#### Task 1.5 — Structured JSON logging
1. Configure logback (`logback.xml`) to emit **JSON to stdout** via `logstash-logback-encoder`.
2. Every line carries contextual fields. Use **MDC** to attach per-event context:
   ```json
   {
     "level": "info",
     "ts": "2026-06-21T07:01:22.451Z",
     "logger": "org.phoenix.ingestion.Processor",
     "msg": "Successfully processed inbound webhook event",
     "event_id": "evt_987654321",
     "event_type": "user.payment_processed",
     "duration_ms": 112,
     "trace_id": "<filled in Phase 3>",
     "span_id": "<filled in Phase 3>"
   }
   ```
3. Leave `trace_id`/`span_id` as placeholders — the OTel agent injects them automatically in Phase 3 (logback MDC integration).
* **Done-test:** Console output is one valid JSON object per processed event with `duration_ms`.

#### Task 1.6 — Multi-stage Dockerfile
1. **Stage 1 (build):** `maven:3.9-eclipse-temurin-17`, run `mvn package`.
2. **Stage 2 (run):** minimal JRE base — `eclipse-temurin:17-jre-alpine` or `gcr.io/distroless/java17-debian12`. Copy only the fat jar.
3. Set a non-root user and a clean `ENTRYPOINT ["java","-jar","/app/app.jar"]`.
* **Done-test:** `docker build` then `docker run`, pipe in the sample JSON, see one structured JSON log line. **Phase 1 complete — entirely offline.**

---

### Phase 2: Infrastructure as Code (IaC) & Cloud Provisioning

Provision all infrastructure declaratively. Pick **one** tool — AWS CDK (TypeScript/Python) or HashiCorp Terraform — and stay with it.

> **Architect's lean:** **Terraform.** Its `destroy` is the clean kill-switch the cost section relies on, and HCL keeps infra visibly separate from app code. CDK is fine if you'd rather write infra in a real language. Decide at the start of this phase.

#### Task 2.1 — Network topography
1. Define a VPC in a **single AZ** to minimize footprint.
2. Small CIDR block, e.g. `10.0.0.0/24`.
3. Two subnets:
   * Public subnet with an Internet Gateway.
   * Isolated private subnet — **no NAT Gateway** (NAT incurs ongoing hourly cost).
* **Done-test:** `plan`/`synth` shows VPC + 2 subnets + IGW, no NAT.

#### Task 2.2 — Storage & decoupling layers
1. DynamoDB table `webhook_events`:
   * Partition (hash) key `event_id` (String).
   * Billing mode **`PAY_PER_REQUEST`** (on-demand) — never provision RCUs/WCUs (static hourly cost).
2. SQS Standard queue `webhook-ingestion-queue`:
   * `VisibilityTimeout` = `180s` (≈3× max processing window).
   * `MessageRetentionPeriod` = `1209600` (14 days).
* **Done-test:** Table and queue exist; queue attributes match.

#### Task 2.3 — API Gateway → SQS direct integration (key senior pattern)
1. Define an API Gateway **HTTP API**.
2. IAM Role granting API Gateway `sqs:SendMessage` on **your** queue only.
3. Route: `POST /v1/webhooks`.
4. Integration type `AWS_PROXY` → SQS `SendMessage`. Map the raw HTTP body into the SQS `MessageBody`, and HTTP headers into SQS `MessageAttributes` (this carries the `traceparent` header — critical for Phase 3).
* **Done-test:** `curl` the deployed URL → `200/202`; SQS message count increments by 1.

#### Task 2.4 — Now wire the real SQS source into the worker
1. Implement the real `MessageSource` (the SQS impl behind the Phase 1 interface) using AWS SDK v2: `ReceiveMessage` with long polling (`waitTimeSeconds=20`, `maxNumberOfMessages` up to 10).
2. After successful processing, call `DeleteMessage`. On failure, let visibility timeout return the message for retry.
* **Done-test:** Run the worker **locally** against the real queue (using your AWS credentials) — it drains messages you `curl` in.

#### Task 2.5 — ECS compute definition
1. ECS Cluster `telemetry-practice-cluster`.
2. Fargate Task Definition: `0.25 vCPU`, `0.5 GB RAM`.
3. **Execution role:** pull from ECR + write CloudWatch logs.
4. **Task role:** `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:GetQueueAttributes` on the queue; `dynamodb:PutItem`, `dynamodb:UpdateItem` on the table.
5. ECS Service: desired count `1`, deploy into the public subnet, auto-assign public IP (so it can reach ECR + the SQS/DynamoDB public endpoints without a NAT).
* **Done-test:** Service runs 1 task; the task drains the queue and writes rows to DynamoDB.

---

### Phase 3: Observability & OpenTelemetry Integration

Instrument deep insight across system boundaries. **In Java, lead with the auto-instrumentation agent and add manual spans only where you want extra detail.**

#### Task 3.1 — Attach the OTel Java agent (do this first)
1. Add the `opentelemetry-javaagent.jar` to the image; launch with `-javaagent:/otel/opentelemetry-javaagent.jar`.
2. Set env: `OTEL_TRACES_EXPORTER=otlp`, `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317`, `OTEL_SERVICE_NAME=webhook-worker`.
3. The agent auto-instruments AWS SDK v2 (SQS receive, DynamoDB put) and injects `trace_id`/`span_id` into logback MDC automatically — filling the Phase 1 placeholders.
* **Done-test:** Logs now carry real `trace_id`/`span_id`; spans appear once the collector (Task 3.4) is live.

#### Task 3.2 — Context propagation across the SQS boundary
Because API Gateway drops the HTTP body into SQS, client trace context can be lost unless carried explicitly.
1. The client/API maps the W3C `traceparent` header into an SQS `MessageAttribute` (set up in Task 2.3).
2. In the worker, extract `traceparent` from the message attributes and use OTel's propagator to start the worker span as a **child** of the HTTP request span. If absent, start a new root trace.
* **Done-test:** A single trace spans API Gateway → SQS → worker → DynamoDB (verified in Phase 4 milestones).

#### Task 3.3 — Optional manual spans for richer detail
1. Wrap the DynamoDB write (or business-logic block) in a manual span when you want attributes the agent won't add:
   * `messaging.system = "sqs"`, `db.system = "dynamodb"`, `webhook.event_type`, `webhook.user_id`.
2. On exception/db failure, `span.recordException(e)` and set status `ERROR`.
* **Done-test:** Failed events show red/error spans in X-Ray with your custom attributes.

#### Task 3.4 — Configure the ADOT sidecar
1. Add a second container to the Fargate task: `public.ecr.aws/aws-observability/aws-otel-collector:latest`.
2. Provide a minimal collector config (env var or SSM) enabling the OTLP receiver and the X-Ray + CloudWatch exporters:
   ```yaml
   receivers:
     otlp:
       protocols:
         grpc:
           endpoint: 0.0.0.0:4317
   exporters:
     awsxray:
     awscloudwatchlogs:
       log_group_name: "/ecs/webhook-worker-otel"
       log_stream_name: "metrics"
   service:
     pipelines:
       traces:  { receivers: [otlp], exporters: [awsxray] }
       metrics: { receivers: [otlp], exporters: [awscloudwatchlogs] }
   ```
3. Worker already exports to `localhost:4317` (Task 3.1) — same task, shared loopback.
* **Done-test:** Traces land in X-Ray; the trace map shows connected nodes.

---

### Phase 4: Delivery, Automation & GitOps Pipelines

Eliminate manual pushes to infrastructure.

#### Task 4.1 — Source registry
1. ECR repository `webhook-worker-app`.
2. Image tag mutability `IMMUTABLE` (preferred); enable scan-on-push.

#### Task 4.2 — GitHub Actions workflow (`.github/workflows/deploy.yml`)
1. **Trigger:** push to `main`.
2. **Auth:** AWS via **OIDC** + an IAM role — no long-lived access keys.
3. **Build stage (Java specifics):**
   * `mvn -B package` (cache `~/.m2`).
   * Log in to ECR; build the multi-stage image tagged with `$GITHUB_SHA`.
   * Push to ECR.
4. **Infra & deploy stage:**
   * Install your IaC CLI; run `plan`/`synth` to verify.
   * `terraform apply --auto-approve` (or `cdk deploy --require-approval never`), passing the new image URI (with the SHA) as a variable.
   * IaC updates the ECS Task Definition → ECS performs a rolling deployment.
* **Done-test:** A commit to `main` builds, pushes, deploys hands-off; new behavior appears in CloudWatch logs.

---

### Phase 5: Migrate the Worker to Go (concurrency deep-dive)

With the architecture proven in Java, port **only the worker container** to Go. Everything else (API Gateway, SQS, DynamoDB, IaC, CI/CD) is untouched — you're swapping one container image. This is where you relearn Go concurrency *with full knowledge of what the code must do*.

#### Task 5.1 — Go module scaffold
1. New `go.mod` in a `worker-go/` subdirectory (keep the Java worker for reference/comparison).
2. Add deps: AWS SDK for Go v2 (`config`, `sqs`, `dynamodb`), `zap` or `zerolog` for structured logs, OTel Go SDK.
* **Done-test:** `go build ./...` succeeds.

#### Task 5.2 — Port core logic + idiomatic concurrency
1. Struct for the payload (`json` tags); processor func mirroring Task 1.3.
2. SQS long-poll loop; fan messages out to a **worker pool of goroutines** bounded by a buffered channel (the idiom that replaces `ExecutorService`). Use `context.Context` for cancellation/timeouts.
* **Done-test:** Locally drains the real queue concurrently; clean shutdown on `SIGTERM`.

#### Task 5.3 — Structured logging + OTel (manual, no agent)
1. Configure `zap`/`zerolog` → JSON stdout with the same fields as Task 1.5.
2. Go has **no auto-instrumentation agent** — add manual spans (`tracer.Start(ctx, "DynamoDB.PutItem", ...)`) and propagate `traceparent` from SQS attributes yourself. This is the deliberate contrast with Java's agent.
* **Done-test:** Trace + log parity with the Java worker.

#### Task 5.4 — Multi-stage Go Dockerfile + cutover
1. Stage 1: `golang:1.22` build a static binary (`CGO_ENABLED=0`).
2. Stage 2: `gcr.io/distroless/static-debian12` — copy only the binary (tiny image).
3. Point the ECS Task Definition / CI build at the Go image; deploy via the existing Phase 4 pipeline.
* **Done-test:** Same Phase 4 pipeline ships the Go image; all Section 4 milestones still pass.

---

## 3. Financial Controls & Cost Containment Strategy

Target: stay inside the AWS Free Tier with a monthly bill of **$0.00**.

```
[Your Project Resources]
          │  (usage metadata)
          ▼
┌────────────────────────────────────────────────────────┐
│ AWS Budgets Engine                                     │
└────────────────────────────────────────────────────────┘
          │  (if month-to-date forecast > $1.00)
          ▼
┌────────────────────────────────────────────────────────┐
│ Amazon SNS                                             │
└────────────────────────────────────────────────────────┘
          │  (immediate delivery)
          ▼
┌────────────────────────────────────────────────────────┐
│ Your email / Discord / Slack webhook                   │
└────────────────────────────────────────────────────────┘
```

1. **Immediate-action budget:** Log into the AWS Console *once* manually. Go to **AWS Budgets** → create a Cost Budget with a fixed **$1.00** threshold and an email alert at 80% of actual or forecasted spend.
2. **IaC kill-switch:** When stepping away for more than a day, tear everything down from the CLI:
   * Terraform: `terraform destroy --auto-approve`
   * CDK: `cdk destroy --force`
   Re-running `apply` rebuilds the exact state in minutes.
3. **Scale-to-zero (keep state, drop compute):** To retain storage/queue/networking but avoid runtime fees (notably public IPv4 hourly charges), set the ECS service `desired_count = 0`. Restore to `1` to resume.

---

## 4. Live System Validation Checklist ("Proof of Work")

Prove behavior through external telemetry, not source review.

### Milestone 1 — Direct ingestion proof
```bash
curl -X POST https://<api-id>.execute-api.<region>.amazonaws.com/v1/webhooks \
  -H "Content-Type: application/json" \
  -d '{"event_id":"evt_test_100","event_type":"user.signup","timestamp":"2026-06-21T07:00:00Z","payload":{"user_id":"usr_99"}}'
```
* **Pass:** HTTP `200`/`202` in well under 40 ms; SQS console message count +1.

### Milestone 2 — Asynchronous distributed trace
* CloudWatch → X-Ray trace map. Find your `curl`'s trace.
* **Pass:** One continuous trace across API Gateway → SQS → ECS worker → DynamoDB write. Disconnected graphs ⇒ fix context propagation (Task 3.2).

### Milestone 3 — Resiliency & backlog
* Set ECS service task count to `0` (simulated worker outage).
* Fire 200 consecutive POSTs into API Gateway.
* **Pass:** Client keeps getting fast `202`s, zero drops; SQS `ApproximateNumberOfMessagesVisible` climbs to 200. Restore task count to `1`; the worker drains the backlog in micro-batches down to 0 with no fatal exceptions.

### Milestone 4 — Continuous delivery
* Change a static log string (e.g. `"Successfully processed inbound webhook event"` → `"Inbound event processed successfully"`).
```bash
git add .
git commit -m "chore: optimize log layout signature"
git push origin main
```
* **Pass:** GitHub Actions builds → pushes to ECR → updates infra → rolling ECS deploy, all hands-off; the new string appears in live CloudWatch logs.

---

## Progress Tracker

- [ ] **Phase 1** — Local Java worker, JSON logging, Dockerized (offline)
- [ ] **Phase 2** — IaC: VPC, SQS, DynamoDB, API Gateway→SQS, ECS Fargate
- [ ] **Phase 3** — OTel Java agent, context propagation, ADOT sidecar
- [ ] **Phase 4** — ECR + GitHub Actions OIDC CD pipeline
- [ ] **Phase 5** — Port worker to Go (concurrency + manual OTel)
- [ ] Cost guardrails: $1 budget alarm set; kill-switch verified
- [ ] Milestones 1–4 validated