(ns s-exp.drip-cli
  (:require [clojure.string :as str]
            [s-exp.drip-cli.commands.jobs :as cmd-jobs]
            [s-exp.drip-cli.commands.migrate :as cmd-migrate]
            [s-exp.drip-cli.commands.queues :as cmd-queues]
            [s-exp.drip-cli.db :as db]
            [s-exp.drip-cli.views.jobs :as view-jobs])
  (:import (com.zaxxer.hikari HikariDataSource))
  (:gen-class))

;; ---------------------------------------------------------------------------
;; Arg parsing
;;
;; Minimal hand-rolled parser: --key value or --flag
;; Returns {:_args [positional...] :key "value" :flag true ...}
;; ---------------------------------------------------------------------------

(defn- parse-args [args]
  (loop [remaining args
         result {}]
    (if (empty? remaining)
      result
      (let [arg (first remaining)]
        (cond
          (and (str/starts-with? arg "--")
               (> (count remaining) 1)
               (not (str/starts-with? (second remaining) "--")))
          (let [k (keyword (subs arg 2))]
            (recur (drop 2 remaining)
                   (assoc result k (second remaining))))

          (str/starts-with? arg "--")
          (let [k (keyword (subs arg 2))]
            (recur (rest remaining) (assoc result k true)))

          :else
          (recur (rest remaining)
                 (update result :_args (fnil conj []) arg)))))))

;; ---------------------------------------------------------------------------
;; Usage
;; ---------------------------------------------------------------------------

(def usage
  "drip-cli — Drip job queue management CLI

Usage:
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

jobs list options:
  --state <s>              Single state filter
  --states <s1,s2,...>     Multiple states (OR)
  --kind <k>               Single kind filter
  --kinds <k1,k2,...>      Multiple kinds (OR)
  --queue <q>              Single queue filter
  --queues <q1,q2,...>     Multiple queues (OR)
  --limit <n>              Max results (default 100)
  --after <id>             Cursor: return jobs with id < after
  --created-after <iso>    ISO-8601 instant lower bound on created_at
  --created-before <iso>   ISO-8601 instant upper bound on created_at
  --scheduled-after <iso>  ISO-8601 instant lower bound on scheduled_at
  --scheduled-before <iso> ISO-8601 instant upper bound on scheduled_at

jobs delete-many options:
  --states <s1,s2,...>     States to delete (required)
  --kinds <k1,k2,...>
  --queues <q1,q2,...>
  --created-before <iso>
  --finalized-before <iso>

Examples:
  drip-cli --url 'jdbc:postgresql://localhost/mydb?user=pg&password=pg' jobs list --state failed
  drip-cli --url 'jdbc:postgresql://localhost/mydb?user=pg&password=pg' jobs tui
  drip-cli --url 'jdbc:postgresql://localhost/mydb?user=pg&password=pg' jobs retry 42
  drip-cli --url 'jdbc:postgresql://localhost/mydb?user=pg&password=pg' jobs retry --id 42
  DRIP_JDBC_URL='jdbc:...' drip-cli jobs list --format json | jq .
  drip-cli --url 'jdbc:...' migrate
")

;; ---------------------------------------------------------------------------
;; Dispatch
;; ---------------------------------------------------------------------------

(defn- exit-usage []
  (println usage)
  (System/exit 0))

(defn- exit-err [msg]
  (println (str "Error: " msg))
  (println "Run drip-cli --help for usage.")
  (System/exit 1))

(defn dispatch [client opts positional]
  (let [group (first positional)
        sub (second positional)
        ;; third positional arg is treated as --id for single-job subcommands
        id-from-pos (get positional 2)
        cmd-opts (cond-> (dissoc opts :url :_args :help)
                   (and id-from-pos (nil? (:id opts))) (assoc :id id-from-pos))]
    (case group
      "jobs"
      (case sub
        "list" (cmd-jobs/list-jobs client cmd-opts)
        "get" (cmd-jobs/get-job client cmd-opts)
        "cancel" (cmd-jobs/cancel-job client cmd-opts)
        "retry" (cmd-jobs/retry-job client cmd-opts)
        "discard" (cmd-jobs/discard-job client cmd-opts)
        "delete" (cmd-jobs/delete-job client cmd-opts)
        "delete-many" (cmd-jobs/delete-jobs client cmd-opts)
        "tui" (view-jobs/run client (dissoc cmd-opts :format))
        (exit-err (str "Unknown jobs subcommand: " sub)))

      "queues"
      (let [queue-opts (cond-> cmd-opts
                         (and id-from-pos (nil? (:name opts))) (assoc :name id-from-pos))]
        (case sub
          "list" (cmd-queues/list-queues client queue-opts)
          "pause" (cmd-queues/pause-queue client queue-opts)
          "resume" (cmd-queues/resume-queue client queue-opts)
          (exit-err (str "Unknown queues subcommand: " sub))))

      "migrate"
      (cmd-migrate/migrate client cmd-opts)

      (exit-err (str "Unknown command: " group)))))

;; ---------------------------------------------------------------------------
;; Main
;; ---------------------------------------------------------------------------

(defn -main [& args]
  (let [opts (parse-args args)
        positional (get opts :_args [])]

    (when (or (:help opts) (empty? positional))
      (exit-usage))

    (let [jdbc-url (or (:url opts) (System/getenv "DRIP_JDBC_URL"))]
      (when-not jdbc-url
        (exit-err "JDBC URL required: use --url or set DRIP_JDBC_URL env var"))

      (let [^HikariDataSource ds (db/make-datasource jdbc-url)]
        (try
          (let [db-type (db/detect-db jdbc-url)
                client (db/make-client ds db-type)]
            (dispatch client (assoc opts :format (get opts :format "table")) positional))
          (finally
            (db/close! ds)))))))
