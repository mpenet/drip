(ns drip-ui.routes
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [drip-ui.views.job-detail :as job-detail]
            [drip-ui.views.jobs :as jobs]
            [drip-ui.views.queue-detail :as queue-detail]
            [drip-ui.views.queues :as queues]
            [s-exp.appia :as appia]
            [s-exp.drip :as drip])
  (:import (java.net URLDecoder URLEncoder)))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- redirect [location]
  {:status 303
   :headers {"location" location}
   :body ""})

(defn- html [body]
  {:status 200
   :headers {"content-type" "text/html; charset=utf-8"}
   :body body})

(defn- not-found []
  {:status 404
   :headers {"content-type" "text/plain"}
   :body "Not found"})

(defn- parse-query-string
  "Parses a query string into a map of string keys → string values."
  [qs]
  (when (seq qs)
    (into {}
          (for [pair (str/split qs #"&")
                :let [[k v] (str/split pair #"=" 2)]
                :when (seq k)]
            [(URLDecoder/decode k "UTF-8")
             (URLDecoder/decode (or v "") "UTF-8")]))))

(defn- flash-from-query [query-params]
  (when-let [msg (get query-params "flash")]
    {:type (or (get query-params "flash_type") "info")
     :msg msg}))

(defn- flash-redirect [location msg type]
  (redirect (str location "?flash=" (URLEncoder/encode msg "UTF-8")
                 "&flash_type=" (name type))))

(defn- serve-static [path]
  (if-let [resource (io/resource (str "public/" path))]
    {:status 200
     :headers {"content-type" (cond
                                (str/ends-with? path ".js")  "text/javascript"
                                (str/ends-with? path ".css") "text/css"
                                (str/ends-with? path ".png") "image/png"
                                (str/ends-with? path ".svg") "image/svg+xml"
                                :else "application/octet-stream")
               "cache-control" "public, max-age=3600"}
     :body (io/input-stream resource)}
    (not-found)))

;; ---------------------------------------------------------------------------
;; Router
;; ---------------------------------------------------------------------------

(def ^:private routes
  {[:get "/"] ::root
   [:get "/public/{*}"] ::static
   [:get "/jobs"] ::jobs-list
   [:get "/jobs/stream"] ::jobs-stream
   [:get "/jobs/{id}"] ::job-detail
   [:post "/jobs/{id}/{action}"] ::job-action
   [:get "/queues"] ::queues-list
   [:get "/queues/{name}"] ::queue-detail
   [:post "/queues/{name}/{action}"] ::queue-action})

(def ^:private router (appia/router routes))

(defn- handlers
  "Returns a map of route keyword → handler fn, closed over client and request context."
  [client request query flash]
  {::root
   (fn [_params]
     (redirect "/queues"))

   ::static
   (fn [params]
     (serve-static (get params :*)))

   ::jobs-list
   (fn [_params]
     (html (jobs/page client query)))

   ::jobs-stream
   (fn [_params]
     (jobs/stream! request client query))

   ::job-detail
   (fn [params]
     (let [job-id (Long/parseLong ^String (:id params))
           result (job-detail/page client job-id flash)]
       (if (map? result) result (html result))))

   ::job-action
   (fn [params]
     (let [job-id (Long/parseLong ^String (:id params))
           action (:action params)]
       (try
         (case action
           "cancel" (drip/cancel-job client job-id)
           "retry" (drip/retry-job client job-id)
           "discard" (drip/discard-job client job-id)
           "delete" (drip/delete-job client job-id))
         (if (= action "delete")
           (flash-redirect "/jobs" (str "Job #" job-id " deleted.") :info)
           (flash-redirect (str "/jobs/" job-id)
                           (str "Job #" job-id " " action "led.")
                           :info))
         (catch Exception e
           (flash-redirect (str "/jobs/" job-id)
                           (str "Error: " (.getMessage e))
                           :error)))))

   ::queues-list
   (fn [_params]
     (html (queues/page client flash)))

   ::queue-detail
   (fn [params]
     (let [result (queue-detail/page client (:name params) flash)]
       (if (map? result) result (html result))))

   ::queue-action
   (fn [params]
     (let [queue-name (:name params)
           action (:action params)]
       (try
         (case action
           "pause" (drip/pause-queue client queue-name)
           "resume" (drip/resume-queue client queue-name))
         (flash-redirect (str "/queues/" queue-name)
                         (str "Queue '" queue-name "' " action "d.")
                         :info)
         (catch Exception e
           (flash-redirect (str "/queues/" queue-name)
                           (str "Error: " (.getMessage e))
                           :error)))))})

(defn handler
  "Returns a Ring handler fn closed over `client`."
  [client]
  (fn [request]
    (let [query (parse-query-string (:query-string request))
          flash (flash-from-query query)]
      (if-let [[route params] (appia/match router request)]
        (if-let [h (get (handlers client request query flash) route)]
          (h params)
          (not-found))
        (not-found)))))
