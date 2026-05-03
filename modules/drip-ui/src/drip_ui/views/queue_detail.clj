(ns drip-ui.views.queue-detail
  (:require [dev.onionpancakes.chassis.core :as h]
            [drip-ui.format :as fmt]
            [drip-ui.views.layout :as layout]
            [next.jdbc :as jdbc]
            [s-exp.drip :as drip]))

(def ^:private state-order
  [:running :available :scheduled :retryable :pending :completed :cancelled :discarded])

(defn- count-jobs-by-state [client queue-name]
  (let [rows (jdbc/execute! (:ds client)
                            ["SELECT state, COUNT(*) AS cnt FROM drip_job WHERE queue = ? GROUP BY state"
                             queue-name]
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

(defn- state-stats [counts]
  (let [total (apply + (vals counts))]
    [:div {:class "stat-grid"}
     (for [state state-order
           :let [n (get counts state 0)]
           :when (pos? n)]
       [:div {:class "stat-card"}
        [:div {:class "stat-value"} n]
        [:div {:class "stat-label"}
         [:span {:class (str "badge badge-" (name state))} (name state)]]])
     [:div {:class "stat-card"}
      [:div {:class "stat-value"} total]
      [:div {:class "stat-label"} "total"]]]))

(defn page
  "Renders the queue detail HTML page."
  [client queue-name flash]
  (let [queue (->> (drip/list-queues client)
                   (filter #(= (:name %) queue-name))
                   first)]
    (if (nil? queue)
      {:status 404
       :headers {"content-type" "text/html; charset=utf-8"}
       :body (layout/page :queues "Not Found" [:p "Queue not found."])}
      (let [paused? (some? (:paused-at queue))
            action (if paused? "resume" "pause")
            action-label (if paused? "Resume" "Pause")
            btn-class (if paused? "btn-primary" "btn-warning")
            counts (count-jobs-by-state client queue-name)]
        {:status 200
         :headers {"content-type" "text/html; charset=utf-8"}
         :body (layout/page
                :queues (str "Queue: " queue-name)
                (list
                 (when flash
                   [:div {:class (str "flash flash-" (:type flash))} (:msg flash)])
                 [:div {:class "page-header"}
                  [:h1 (str "Queue: " queue-name)]
                  [:a {:href "/queues" :class "btn btn-secondary"} "← Back"]]
                 [:div {:class "action-bar"}
                  [:form {:method "post" :action (str "/queues/" queue-name "/" action) :style "display:inline"}
                   [:button {:type "submit" :class (str "btn " btn-class)} action-label]]
                  [:a {:href (str "/jobs?queue=" queue-name) :class "btn btn-secondary"}
                   "View Jobs"]]
                 [:table {:class "detail-table"}
                  [:tbody
                   [:tr [:th "Name"] [:td queue-name]]
                   [:tr [:th "Status"] [:td (if paused?
                                              (list
                                               [:span {:class "badge badge-cancelled"} "paused"]
                                               " "
                                               [:small "since " (fmt/fmt-instant (:paused-at queue))])
                                              [:span {:class "badge badge-available"} "active"])]]
                   [:tr [:th "Created At"] [:td (fmt/fmt-instant (:created-at queue))]]
                   [:tr [:th "Updated At"] [:td (fmt/fmt-instant (:updated-at queue))]]
                   [:tr [:th "Metadata"] [:td [:pre {:class "code"} (str (:metadata queue))]]]]]
                 (when (seq counts)
                   (list
                    [:h2 {:class "section-title"} "Job counts"]
                    (state-bar counts)
                    (state-stats counts)))))}))))
