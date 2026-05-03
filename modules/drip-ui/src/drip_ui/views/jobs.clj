(ns drip-ui.views.jobs
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [dev.onionpancakes.chassis.core :as h]
            [drip-ui.datastar :as ds]
            [drip-ui.format :as fmt]
            [drip-ui.views.layout :as layout]
            [next.jdbc :as jdbc]
            [s-exp.drip :as drip]
            [s-exp.hirundo.sse :as sse]))

(def ^:private page-size 50)

(def ^:private state-order
  [:running :available :scheduled :retryable :pending :completed :cancelled :discarded])

(defn- count-jobs-by-state [client]
  (let [rows (jdbc/execute! (:ds client)
                            ["SELECT state, COUNT(*) AS cnt FROM drip_job GROUP BY state"]
                            {:builder-fn next.jdbc.result-set/as-unqualified-kebab-maps})]
    (into {} (map (fn [r] [(keyword (:state r)) (:cnt r)]) rows))))

(defn- state-bar [counts]
  (let [total (max 1 (apply + (vals counts)))]
    [:div {:class "state-bar"}
     (for [state state-order
           :let [n (get counts state 0)]
           :when (pos? n)]
       [:div {:class (str "state-bar-segment state-bar-" (name state))
              :style (str "width:" (/ (* n 100.0) total) "%")
              :data-tip (str (name state) ": " n)}])]))

(defn- state-bar-html [counts]
  (h/html (state-bar counts)))

(def ^:private all-states
  ["" "available" "running" "retryable" "scheduled"
   "pending" "completed" "discarded" "cancelled"])

(def ^:private all-priorities ["" "1" "2" "3" "4"])

(defn- build-url [base-path params]
  (let [qs (str/join "&"
                     (for [[k v] params
                           :when (seq (str v))]
                       (str (name k) "=" v)))]
    (if (seq qs) (str base-path "?" qs) base-path)))

(defn- filter-select [name-attr current opts label-fn]
  [:select {:class "state-filter" :name name-attr :onchange "this.form.submit()"}
   (for [o opts]
     [:option (cond-> {:value o}
                (= o (or current "")) (assoc :selected true))
      (label-fn o)])])

(defn- filter-text [name-attr current placeholder]
  [:input {:type "text" :name name-attr :value (or current "")
           :placeholder placeholder
           :class "filter-text"
           :onchange "this.form.submit()"}])

(defn- job-row [job]
  [:tr {:class "clickable-row"
        :onclick (str "window.location='/jobs/" (:id job) "'")}
   [:td (:id job)]
   [:td (:kind job)]
   [:td (:queue job)]
   [:td (fmt/state-badge (:state job))]
   [:td (:priority job)]
   [:td (str (:attempt job) "/" (:max-attempts job))]
   [:td (fmt/fmt-instant (:scheduled-at job))]
   [:td (fmt/fmt-instant (:created-at job))]])

(defn- jobs-table-body [jobs]
  [:tbody {:id "jobs-tbody"}
   (if (seq jobs)
     (map job-row jobs)
     [:tr [:td {:colspan 8 :class "empty"} "No jobs found."]])])

(defn- pagination-bar [jobs params]
  (let [has-next? (> (count jobs) page-size)
        next-after (when has-next? (:id (last (take page-size jobs))))
        prev? (some? (:after params))]
    [:div {:class "pagination"}
     (if prev?
       [:a {:href (build-url "/jobs" (dissoc params :after)) :class "btn btn-secondary btn-sm"} "← First"]
       [:span {:class "btn btn-secondary btn-sm disabled"} "← First"])
     (if has-next?
       [:a {:href (build-url "/jobs" (assoc params :after next-after)) :class "btn btn-secondary btn-sm"} "Next →"]
       [:span {:class "btn btn-secondary btn-sm disabled"} "Next →"])]))

(defn- parse-filters [{:strs [state kind queue priority after]}]
  (cond-> {}
    (seq state) (assoc :state state)
    (seq kind) (assoc :kind kind)
    (seq queue) (assoc :queue queue)
    (seq priority) (assoc :priority priority)
    (seq after) (assoc :after (Long/parseLong after))))

(defn- list-jobs-page [client filters]
  (let [opts (cond-> {:limit (inc page-size)}
               (:state filters) (assoc :state (keyword (:state filters)))
               (:kind filters) (assoc :kind (:kind filters))
               (:queue filters) (assoc :queue (:queue filters))
               (:priority filters) (assoc :priorities [(Long/parseLong (:priority filters))])
               (:after filters) (assoc :after (:after filters)))]
    (drip/list-jobs client opts)))

(defn- filter-bar [filters]
  [:form {:method "get" :action "/jobs" :class "filter-form"}
   (filter-select "state" (:state filters) all-states #(if (= % "") "All states" %))
   (filter-text "kind" (:kind filters) "Kind…")
   (filter-text "queue" (:queue filters) "Queue…")
   (filter-select "priority" (:priority filters) all-priorities #(if (= % "") "All priorities" (str "Priority " %)))
   [:button {:type "submit" :class "btn btn-secondary btn-sm"} "Filter"]
   (when (some seq (vals filters))
     [:a {:href "/jobs" :class "btn btn-secondary btn-sm"} "Clear"])])

(defn page
  "Renders the full jobs list HTML page."
  [client query-params]
  (let [filters (parse-filters query-params)
        jobs (list-jobs-page client filters)
        displayed (take page-size jobs)
        first-page? (nil? (:after filters))
        stream-params (dissoc filters :after)
        stream-url (when first-page? (build-url "/jobs/stream" stream-params))
        counts (when first-page? (count-jobs-by-state client))]
    (layout/page
     :jobs "Jobs"
     (list
      [:div {:class "page-header"}
       [:h1 "Jobs"]
       (filter-bar filters)]
      (when first-page?
        [:div {:id "state-bar-wrapper"} (state-bar counts)])
      [:div {:class "table-wrapper"}
       [:table {:class "data-table"}
        [:thead
         [:tr
          [:th "ID"] [:th "Kind"] [:th "Queue"] [:th "State"]
          [:th "Priority"] [:th "Attempt"] [:th "Scheduled At"] [:th "Created At"]]]
        (jobs-table-body displayed)]]
      (pagination-bar jobs filters)
      (if first-page?
        (list
         [:p {:class "auto-refresh"}
          [:span {:class "live-dot"}]
          [:span {:id "sse-status"} "Connecting…"]]
         [:div {:data-init (str "@get('" stream-url "')") :style "display:none"}])
        [:p {:class "auto-refresh"} "Browsing history — no live updates on paginated pages."])))))

(defn- jobs-table-body-html [jobs]
  (h/html (jobs-table-body jobs)))

(defn stream!
  "SSE handler — pushes first-page table body updates every 5 seconds."
  [request client query-params]
  (let [filters (parse-filters query-params)]
    (with-open [s (sse/stream! request)]
      (loop []
        (let [jobs (list-jobs-page client filters)
              displayed (take page-size jobs)
              counts (count-jobs-by-state client)
              tbody (jobs-table-body-html displayed)
              bar-html (h/html [:div {:id "state-bar-wrapper"} (state-bar counts)])
              status-html (h/html [:span {:id "sse-status"} "Live"])]
          (when (and (async/>!! (:input-ch s)
                                (ds/patch-elements [tbody] :selector "#jobs-tbody" :mode :outer))
                     (async/>!! (:input-ch s)
                                (ds/patch-elements [bar-html] :selector "#state-bar-wrapper" :mode :outer))
                     (async/>!! (:input-ch s)
                                (ds/patch-elements [status-html] :selector "#sse-status" :mode :outer)))
            (Thread/sleep 5000)
            (recur)))))))
