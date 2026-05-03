(ns drip-ui.views.job-detail
  (:require [dev.onionpancakes.chassis.core :as h]
            [drip-ui.format :as fmt]
            [drip-ui.views.layout :as layout]
            [jsonista.core :as json]
            [s-exp.drip :as drip]))

(defn- render-errors [errors]
  (if (seq errors)
    [:ul {:class "error-list"}
     (for [err errors]
       [:li
        [:strong (str (get err "error" (get err :error "unknown")))]
        (when-let [t (or (get err "at") (get err :at))]
          [:span {:class "ts"} (str t)])])]
    [:em "none"]))

(defn- action-button [job-id action label css-class]
  [:form {:method "post" :action (str "/jobs/" job-id "/" action) :style "display:inline"}
   [:button {:type "submit" :class (str "btn " css-class)} label]])

(defn- action-buttons [job]
  (let [state (:state job)
        id (:id job)
        retryable? (#{:discarded :cancelled :failed :retryable} state)
        cancellable? (#{:available :scheduled :retryable :pending} state)
        discardable? (#{:running :retryable :available :scheduled} state)]
    [:div {:class "action-bar"}
     (when retryable? (action-button id "retry" "Retry" "btn-primary"))
     (when cancellable? (action-button id "cancel" "Cancel" "btn-warning"))
     (when discardable? (action-button id "discard" "Discard" "btn-danger"))
     (action-button id "delete" "Delete" "btn-danger btn-outline")]))

(defn- field-row [label value]
  [:tr [:th label] [:td value]])

(defn page
  "Renders the job detail HTML page."
  [client job-id flash]
  (let [job (drip/get-job client job-id)]
    (if (nil? job)
      {:status 404
       :headers {"content-type" "text/html; charset=utf-8"}
       :body (layout/page :jobs "Not Found" [:p "Job not found."])}
      {:status 200
       :headers {"content-type" "text/html; charset=utf-8"}
       :body (layout/page
              :jobs (str "Job #" job-id)
              (list
               (when flash
                 [:div {:class (str "flash flash-" (:type flash))} (:msg flash)])
               [:div {:class "page-header"}
                [:h1 (str "Job #" job-id)]
                [:a {:href "/jobs" :class "btn btn-secondary"} "← Back"]]
               (action-buttons job)
               [:table {:class "detail-table"}
                [:tbody
                 (field-row "ID" (:id job))
                 (field-row "Kind" (:kind job))
                 (field-row "Queue" [:a {:href (str "/queues/" (:queue job))} (:queue job)])
                 (field-row "State" (fmt/state-badge (:state job)))
                 (field-row "Priority" (:priority job))
                 (field-row "Attempt" (str (:attempt job) " / " (:max-attempts job)))
                 (field-row "Attempted By" (str (:attempted-by job)))
                 (field-row "Created At" (fmt/fmt-instant (:created-at job)))
                 (field-row "Scheduled At" (fmt/fmt-instant (:scheduled-at job)))
                 (field-row "Attempted At" (fmt/fmt-instant (:attempted-at job)))
                 (field-row "Finalized At" (fmt/fmt-instant (:finalized-at job)))
                 (field-row "Tags" (str (:tags job)))
                 (field-row "Args" [:pre {:class "code"} (json/write-value-as-string (:args job))])
                 (field-row "Metadata" [:pre {:class "code"} (json/write-value-as-string (:metadata job))])
                 (field-row "Errors" (render-errors (:errors job)))]]))})))
