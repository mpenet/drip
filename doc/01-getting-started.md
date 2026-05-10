# Getting Started

## What is Drip?

Drip is a transactional job queue for Clojure backed by MariaDB, PostgreSQL, or SQLite. Jobs are inserted inside your application's own database transactions, so a job only exists if the transaction that created it committed. No external services, no message brokers, no dual-write risk.

**Requirements:** Java 21+, one of: MariaDB 10.6+, PostgreSQL 10+, SQLite 3.38.0+.

## Installation

```clojure
;; deps.edn
com.s-exp/drip {:mvn/version "0.1.0"}
```

Add your JDBC driver:

```clojure
;; MariaDB 10.6+
org.mariadb.jdbc/mariadb-java-client {:mvn/version "3.3.3"}

;; PostgreSQL 10+
org.postgresql/postgresql {:mvn/version "42.7.3"}

;; SQLite 3.38.0+
org.xerial/sqlite-jdbc {:mvn/version "3.45.3.0"}
```

## Minimal example

```clojure
(ns myapp.jobs
  (:require [s-exp.drip :as drip]
            [s-exp.drip.client.postgres :as postgres]
            [next.jdbc.connection :as jdbc-conn])
  (:import (com.zaxxer.hikari HikariDataSource)))

;; 1. Create a DataSource (HikariCP or any javax.sql.DataSource)
(def ^HikariDataSource ds
  (jdbc-conn/->pool HikariDataSource
    {:jdbcUrl "jdbc:postgresql://localhost/myapp"
     :username "myuser"
     :password "secret"}))

;; 2. Create a Drip client
(def client (postgres/make-client ds))

;; 3. Run migrations (idempotent — safe on every startup)
(drip/migrate! client)

;; 4. Define your worker registry: kind string → (fn [client job] ...)
(def registry
  {"send_email"
   (fn [_ job]
     (send-email! (get-in job [:args :to])
                  (get-in job [:args :subject])
                  (get-in job [:args :body])))})

;; 5. Start the worker
(def worker
  (drip/start-worker!
    {:client   client
     :registry registry}))

;; 6. Enqueue a job (auto-completes when the handler returns normally)
(drip/insert-job client "send_email"
  {:to      "user@example.com"
   :subject "Welcome!"
   :body    "Thanks for signing up."})

;; 7. Shut down gracefully (waits up to 30s for in-flight jobs)
(drip/stop-worker! executor)
```

## Creating a client

Each database dialect has its own `make-client` constructor. All three take a `javax.sql.DataSource`:

```clojure
(require '[s-exp.drip.client.mariadb  :as mariadb])
(require '[s-exp.drip.client.postgres :as postgres])
(require '[s-exp.drip.client.sqlite   :as sqlite])

(def client (mariadb/make-client  datasource))
(def client (postgres/make-client datasource))
(def client (sqlite/make-client   datasource))
```

The client record is passed to every Drip function. It holds the datasource and dialect-specific SQL implementations.

## Running migrations

```clojure
(drip/migrate! client)
```

Creates three tables — `drip_job`, `drip_queue`, `drip_migration` — and the indexes needed for efficient fetching. Managed by [migratus](https://github.com/yogthos/migratus); fully idempotent. Call it on every application startup.

Migration SQL lives in `resources/migrations/<dialect>/001-initial-schema.up.sql` in the Drip jar. You can inspect it there.

## Component lifecycle

A typical component-based startup (works with Component, Integrant etc):

```clojure
;; Start
(def client   (postgres/make-client ds))
(drip/migrate! client)
(def worker (drip/start-worker! {:client client :registry registry}))

;; Stop
(drip/stop-worker! worker)   ; waits up to 30s for in-flight jobs
```

For immediate shutdown that doesn't wait for in-flight jobs:

```clojure
(drip/stop-and-cancel! executor)
;; In-flight jobs remain :running; they will be rescued on next executor startup.
```

## Namespace overview

| Namespace | Purpose |
|---|---|
| `s-exp.drip` | Public API — use this for everything |
| `s-exp.drip.client.mariadb` | MariaDB client constructor |
| `s-exp.drip.client.postgres` | PostgreSQL client constructor |
| `s-exp.drip.client.sqlite` | SQLite client constructor |
| `s-exp.drip.job` | Job record, states, retry policy |
| `s-exp.drip.worker` | Executor internals (rarely needed directly) |
| `s-exp.drip.periodic` | Periodic job scheduler internals |

In nearly all cases you only need `s-exp.drip` and one dialect namespace.
