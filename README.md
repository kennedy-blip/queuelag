# QueueLag

QueueLag is a Go + Vanilla JavaScript message-queue backlog, retry-pressure, and dead-letter queue simulator.

## Features

- Pre-seeded M-Pesa STK callback, webhook relay, and ledger worker queue profiles.
- Configurable ingress, worker concurrency, processing latency, failure rate, retry limit, and simulation steps.
- Per-step backlog, completion, retry, DLQ, throughput, and drain-time telemetry.
- Queue creation/reset and DLQ recovery guidance.
- Browser-only demo mode when the API is unavailable.

## Run locally

Requires Go 1.23+.

```sh
go run ./local.go
```

The Vercel functions are exposed at `/api/queues` and `/api/step`. For local development, use Vercel CLI (`vercel dev`) or provide an equivalent Go HTTP server. The UI automatically falls back to a client-side simulator when API requests fail.

## Deploy to Vercel

Import this repository into Vercel. The project is a static frontend plus Go serverless functions; no Node bundler or TypeScript build is required.

## State and simulation model

The supplied API stores queue profiles in process memory. Serverless instances can be recycled or scaled independently, so this is a simulator rather than durable queue infrastructure: state may reset between invocations or differ across instances. Use a shared database or cache for persistent, cross-instance state in a production deployment.

The simulator is educational and uses approximate per-minute capacity and retry calculations. It does not connect to M-Pesa, a message broker, or production worker systems.
