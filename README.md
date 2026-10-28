# Streaming Workshop – Redpanda and PyFlink

Hands-on stream processing from DataTalksClub's PyFlink workshop ([guide](workshop/guide.md), [homework](workshop/homework.md)). Trip events go into a Kafka-compatible broker (Redpanda), Flink jobs aggregate them in time windows, and the results are written to Postgres.

Built in March 2026. Data: NYC green taxi trips (public TLC data).

## How it fits together

```
Python producer ──► Redpanda (topic: rides / green-trips) ──► PyFlink job ──► Postgres
```

| Job (`src/job/`) | What it does |
|---|---|
| `pass_through_job.py` | Reads the `rides` topic and writes the rows to Postgres unchanged |
| `aggregation_job.py` | Per pickup zone, per 1-hour tumbling window: trip count and total revenue |
| `tumbling_job.py` | Per pickup zone, per 5-minute tumbling window: trip count (`green-trips` topic) |
| `session_job.py` | Per pickup zone, session windows: trip count |
| `largest_tip_job.py` | Total tip amount per 1-hour tumbling window |

All jobs use event time from the pickup timestamp, with a 5-second watermark.

`src/producers/producer_realtime.py` sends simulated rides to Redpanda. The `Homework/` folders have the producer and consumer for the green taxi data, plus my answers.

## Tech stack

- Redpanda (Kafka API)
- Apache Flink 2.2 with PyFlink (Python 3.12), Flink SQL
- PostgreSQL 18
- Docker Compose; Python packages managed with `uv`
- `kafka-python`, pandas, psycopg2

## Run locally

Requires Docker and [uv](https://docs.astral.sh/uv/).

```bash
docker compose build
docker compose up -d
```

This starts Redpanda, Postgres, and a Flink job manager and task manager (Flink UI at http://localhost:8081). Then send some data and submit a job, following the steps in [workshop/guide.md](workshop/guide.md).

The Postgres password in `docker-compose.yml` is a local-only default.

## Project structure

```
src/job/          PyFlink jobs
src/producers/    producer and ride model
notebooks/        producer and consumer notebooks
Homework/         homework code and answers
workshop/         workshop guide and homework instructions
docker-compose.yml, Dockerfile.flink, flink-config.yaml
```
