# drip-ui

Web UI for [drip](../drip) — browse and manage jobs and queues in real time.

## Features

- **Jobs list** — filterable by state, kind, queue, and priority; live auto-refresh via SSE
- **Job detail** — all fields, error history, and one-click actions (retry, cancel, discard, delete)
- **Queues list** — all queues with pause/active status
- **Queue detail** — per-state job counts with visual distribution bar, pause/resume

<img width="1624" height="1061" alt="Screenshot 2026-05-04 at 20 53 26" src="https://github.com/user-attachments/assets/2fe918f3-3784-403e-be4f-f82d57005c77" />
<img width="1624" height="1061" alt="Screenshot 2026-05-04 at 20 53 34" src="https://github.com/user-attachments/assets/cd18821a-20a7-4a93-8643-a954d5a418cf" />
<img width="1624" height="1061" alt="Screenshot 2026-05-04 at 20 53 43" src="https://github.com/user-attachments/assets/51b7c20f-9049-4367-8882-713c76b76f4c" />
<img width="1624" height="1061" alt="Screenshot 2026-05-04 at 20 53 56" src="https://github.com/user-attachments/assets/78816437-965f-42da-8dc5-32a8081b17bf" />


## Requirements

- Java 21+
- PostgreSQL or MariaDB (same DB used by the drip worker)

## Running locally

```bash
cd modules/drip-ui
DRIP_JDBC_URL=jdbc:postgresql://localhost:5432/mydb?user=postgres&password=postgres \
  clojure -M:run
```

Open http://localhost:8080. The jobs list auto-refreshes every 5 seconds on page 1.

## Docker

The Dockerfile is designed to be built from the **repository root**:

```bash
# From repo root
docker build -f modules/drip-ui/Dockerfile -t drip-ui .

docker run -e DRIP_JDBC_URL=jdbc:postgresql://host:5432/mydb?user=u&password=p \
           -p 8080:8080 drip-ui
```

The multi-stage build compiles an uberjar in the builder stage and copies only the JRE + jar to the final image.

## Building the uberjar

```bash
cd modules/drip-ui
clojure -T:build uber
java -jar target/drip-ui.jar
```

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `DRIP_JDBC_URL` | Yes | JDBC URL for the drip database |

The dialect (PostgreSQL or MariaDB) is inferred from the URL prefix.

## Routes

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Redirect to `/queues` |
| `GET` | `/queues` | Queue list |
| `GET` | `/queues/:name` | Queue detail with job counts |
| `POST` | `/queues/:name/pause` | Pause a queue |
| `POST` | `/queues/:name/resume` | Resume a queue |
| `GET` | `/jobs` | Job list (filterable, paginated) |
| `GET` | `/jobs/stream` | SSE stream for live job list updates |
| `GET` | `/jobs/:id` | Job detail |
| `POST` | `/jobs/:id/retry` | Retry a job |
| `POST` | `/jobs/:id/cancel` | Cancel a job |
| `POST` | `/jobs/:id/discard` | Discard a job |
| `POST` | `/jobs/:id/delete` | Delete a job |
