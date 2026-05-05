# drip-cli

A command-line tool for managing [Drip](../../README.md) job queues. Supports PostgreSQL, MariaDB, and SQLite. Includes both a plain-text output mode, JSON output for scripting, and an interactive TUI browser built with [charm.clj](https://github.com/TimoKramer/charm.clj).

**Requires Java 21+.**

## Installation

Build an uberjar:

```bash
clojure -T:build uber
# produces target/drip-cli.jar
```

Run:

```bash
java -jar target/drip-cli.jar --url 'jdbc:postgresql://localhost/mydb?user=pg&password=pg' jobs list
```

Or via alias during development:

```bash
clojure -M:run --url 'jdbc:postgresql://localhost/mydb?user=pg&password=pg' jobs list
```

## Connection

Provide the JDBC URL via `--url` or the `DRIP_JDBC_URL` environment variable. The DB type (postgres/mariadb/sqlite) is detected automatically from the URL.

```bash
export DRIP_JDBC_URL='jdbc:postgresql://localhost/mydb?user=app&password=secret'
drip-cli jobs list
```

## Usage

```
drip-cli [global-opts] <command> [command-opts]

Global options:
  --url <jdbc-url>    JDBC URL (or set DRIP_JDBC_URL env var)
  --format <fmt>      Output format: table (default) | json

Commands:
  jobs list           List jobs
  jobs get            Get a single job by ID (--id <id>)
  jobs cancel         Cancel a job (--id <id>)
  jobs retry          Retry a job (--id <id>)
  jobs discard        Discard a job (--id <id>)
  jobs delete         Delete a single job (--id <id>)
  jobs delete-many    Delete multiple jobs by filter (--states <s1,s2>)
  jobs tui            Interactive TUI browser (--refresh <seconds>, default 5)

  queues list         List queues
  queues pause        Pause a queue (--name <n>)
  queues resume       Resume a queue (--name <n>)

  migrate             Run database migrations
```

## Commands

### Jobs

**List jobs:**

```bash
drip-cli jobs list
drip-cli jobs list --state failed
drip-cli jobs list --states running,retryable
drip-cli jobs list --kind send_email --limit 50
drip-cli jobs list --queue priority --format json | jq '.[] | .id'
drip-cli jobs list --created-after 2026-01-01T00:00:00Z
```

Filter options:

| Option | Description |
|---|---|
| `--state <s>` | Single state: `available`, `running`, `completed`, `retryable`, `discarded`, `cancelled`, `scheduled` |
| `--states <s1,s2>` | Multiple states (OR) |
| `--kind <k>` | Single kind filter |
| `--kinds <k1,k2>` | Multiple kinds (OR) |
| `--queue <q>` | Single queue filter |
| `--queues <q1,q2>` | Multiple queues (OR) |
| `--limit <n>` | Max results (default 100) |
| `--after <id>` | Cursor pagination: return jobs with id < after |
| `--created-after <iso>` | ISO-8601 lower bound on `created_at` |
| `--created-before <iso>` | ISO-8601 upper bound on `created_at` |
| `--scheduled-after <iso>` | ISO-8601 lower bound on `scheduled_at` |
| `--scheduled-before <iso>` | ISO-8601 upper bound on `scheduled_at` |

**Get a job:**

```bash
drip-cli jobs get --id 42
drip-cli jobs get --id 42 --format json
```

**Transition a job:**

```bash
drip-cli jobs cancel  --id 42
drip-cli jobs retry   --id 42
drip-cli jobs discard --id 42
drip-cli jobs delete  --id 42
```

**Bulk delete:**

```bash
# Delete all completed and cancelled jobs older than a date
drip-cli jobs delete-many --states completed,cancelled --finalized-before 2026-01-01T00:00:00Z

# Delete discarded jobs of a specific kind
drip-cli jobs delete-many --states discarded --kinds send_email
```

Options for `delete-many`:

| Option | Description |
|---|---|
| `--states <s1,s2>` | States to delete (required) |
| `--kinds <k1,k2>` | Restrict to these kinds |
| `--queues <q1,q2>` | Restrict to these queues |
| `--created-before <iso>` | Only delete created before this instant |
| `--finalized-before <iso>` | Only delete finalized before this instant |

**Interactive TUI:**

```bash
drip-cli jobs tui
drip-cli jobs tui --refresh 10   # auto-refresh every 10 seconds (default: 5)
```

The TUI shows a scrollable jobs table with auto-refresh. Job detail view shows all fields grouped by section (identity, state, timestamps, args, metadata, errors) with stack trace previews for failed jobs.

Keyboard shortcuts:

| Key | Action |
|---|---|
| `j` / `↓` / `ctrl+n` | Move down |
| `k` / `↑` / `ctrl+p` | Move up |
| `ctrl+d` / `pgdn` | Page down |
| `ctrl+u` / `pgup` | Page up |
| `g` / `home` | Jump to top |
| `G` / `end` | Jump to bottom |
| `enter` | View job detail |
| `r` | Refresh now (resets auto-refresh timer) |
| `a` | Toggle auto-refresh on/off |
| `q` | Quit / back |
| `esc` | Back to list (from detail) |

### Queues

```bash
drip-cli queues list
drip-cli queues list --format json
drip-cli queues pause  --name default
drip-cli queues resume --name default
```

### Migrations

Run pending migrations (idempotent):

```bash
drip-cli migrate
```

## JSON output

All `list` and `get` commands accept `--format json`. Output goes to stdout and is suitable for piping to `jq`:

```bash
# IDs of all running jobs
drip-cli jobs list --state running --format json | jq '.[].id'

# Count jobs by state
drip-cli jobs list --limit 1000 --format json | jq 'group_by(.state) | map({state: .[0].state, count: length})'

# Find jobs that errored more than 3 times
drip-cli jobs list --state discarded --format json | jq '.[] | select(.attempt > 3)'
```

## Building

```bash
# Uberjar
clojure -T:build uber

# Run from source
clojure -M:run --help
```

## License

Copyright © 2026 Max Penet
Distributed under the Mozilla Public License Version 2.0
