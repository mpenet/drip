(ns s-exp.drip-cli.format
  (:require [jsonista.core :as json])
  (:import (java.time Instant ZoneOffset)
           (java.time.format DateTimeFormatter)))

;; ---------------------------------------------------------------------------
;; Instant formatting
;; ---------------------------------------------------------------------------

(def ^:private ^DateTimeFormatter dt-fmt
  (-> (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss")
      (.withZone ZoneOffset/UTC)))

(defn fmt-instant [^Instant inst]
  (if inst (.format dt-fmt inst) ""))

;; ---------------------------------------------------------------------------
;; State coloring (ANSI, only in TUI/table mode)
;; ---------------------------------------------------------------------------

(def state-colors
  {:available "\033[32m" ; green
   :running "\033[34m" ; blue
   :completed "\033[90m" ; dark gray
   :scheduled "\033[33m" ; yellow
   :retryable "\033[33m" ; yellow
   :discarded "\033[31m" ; red
   :cancelled "\033[31m" ; red
   :pending "\033[36m"}) ; cyan

(def reset "\033[0m")

(defn state-str [state color?]
  (let [s (name state)]
    (if color?
      (str (get state-colors state "") s reset)
      s)))

;; ---------------------------------------------------------------------------
;; JSON output
;; ---------------------------------------------------------------------------

(def ^:private json-mapper
  (json/object-mapper {:encode-key-fn name}))

(defn print-json [v]
  (println (json/write-value-as-string v json-mapper)))

;; ---------------------------------------------------------------------------
;; Table output (plain text, no charm TUI)
;; ---------------------------------------------------------------------------

(defn- pad [^String s ^long width]
  (let [len (.length s)]
    (if (>= len width)
      (.substring s 0 width)
      (str s (apply str (repeat (- width len) " "))))))

(defn print-table
  "Prints rows as a plain-text table. columns is [{:key k :title t :width w}]."
  [columns rows]
  (let [header (apply str (map (fn [{:keys [title width]}] (pad title width)) columns))
        sep (apply str (map (fn [{:keys [width]}] (apply str (repeat width "-"))) columns))]
    (println header)
    (println sep)
    (doseq [row rows]
      (println (apply str (map (fn [{:keys [key width fmt]}]
                                 (let [v (get row key)
                                       s (if fmt (fmt v) (str v))]
                                   (pad (or s "") width)))
                               columns))))))

;; ---------------------------------------------------------------------------
;; Job columns
;; ---------------------------------------------------------------------------

(def job-columns
  [{:key :id :title "ID" :width 8}
   {:key :kind :title "KIND" :width 22}
   {:key :queue :title "QUEUE" :width 14}
   {:key :state :title "STATE" :width 12 :fmt #(when % (name %))}
   {:key :priority :title "PRI" :width 5}
   {:key :attempt :title "ATT" :width 5}
   {:key :max-attempts :title "MAX" :width 5}
   {:key :scheduled-at :title "SCHEDULED" :width 22 :fmt fmt-instant}
   {:key :created-at :title "CREATED" :width 22 :fmt fmt-instant}])

(def job-detail-keys
  [:id :kind :queue :state :priority :attempt :max-attempts
   :tags :args :metadata :errors
   :scheduled-at :attempted-at :finalized-at :created-at])

(defn print-job-detail [job]
  (doseq [k job-detail-keys]
    (when-let [v (get job k)]
      (printf "%-16s %s%n" (name k) (str v))))
  (flush))

;; ---------------------------------------------------------------------------
;; Queue columns
;; ---------------------------------------------------------------------------

(def queue-columns
  [{:key :name :title "NAME" :width 30}
   {:key :paused :title "PAUSED" :width 8 :fmt str}
   {:key :created-at :title "CREATED" :width 22 :fmt fmt-instant}])
