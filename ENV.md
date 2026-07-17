# RelayQ Environment Variables

## Required
| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection string. Supports all redis-py URL formats (`redis://`, `rediss://`, `unix://`) |

## Optional
| Variable | Default | Description |
|----------|---------|-------------|
| `RELAYQ_LOG_LEVEL` | `info` | Logging level: `debug`, `info`, `warning`, `error` |
| `RELAYQ_API_URL` | `http://localhost:8000` | Base URL for the CLI client |
| `RELAYQ_MAX_QUEUE_DEPTH` | `10000` | Maximum entries per queue before backpressure kicks in |
| `RELAYQ_WORKER_CONCURRENCY` | `10` | Max concurrent job handlers per worker |
| `RELAYQ_JOB_TIMEOUT` | `300` | Per-job processing timeout in seconds |
| `RELAYQ_RATE_LIMIT` | `100` | API rate limit (requests/second) |
| `RELAYQ_RATE_LIMIT_BURST` | `150` | API burst capacity |
| `RELAYQ_DLQ_RETENTION_HOURS` | `168` | How long to keep DLQ entries before trimming (7 days) |

## Redis
| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_SOCKET_TIMEOUT` | `10` | Redis socket timeout (seconds) |
| `REDIS_RETRY_ON_TIMEOUT` | `true` | Whether to retry on Redis timeout |
| `REDIS_MAX_CONNECTIONS` | `20` | Max Redis connection pool size |
