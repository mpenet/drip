(ns drip-ui.views.queues
  (:require [dev.onionpancakes.chassis.core :as h]
            [drip-ui.format :as fmt]
            [drip-ui.views.layout :as layout]
            [s-exp.drip :as drip]))

(defn- queue-row [queue]
  (let [qname (:name queue)
        paused? (some? (:paused-at queue))
        action (if paused? "resume" "pause")
        action-label (if paused? "Resume" "Pause")
        btn-class (if paused? "btn-primary" "btn-warning")]
    [:tr {:class "clickable-row"
          :onclick (str "window.location='/queues/" qname "'")}
     [:td qname]
     [:td
      (if paused?
        (list
         [:span {:class "badge badge-cancelled"} "paused"]
         " "
         [:small "since " (fmt/fmt-instant (:paused-at queue))])
        [:span {:class "badge badge-available"} "active"])]
     [:td (fmt/fmt-instant (:created-at queue))]
     [:td
      [:form {:method "post" :action (str "/queues/" qname "/" action) :style "display:inline"}
       [:button {:type "submit" :class (str "btn " btn-class " btn-sm")} action-label]]]]))

(defn page
  "Renders the queues list HTML page."
  [client flash]
  (let [queues (drip/list-queues client)]
    (layout/page
     :queues "Queues"
     (list
      (when flash
        [:div {:class (str "flash flash-" (:type flash))} (:msg flash)])
      [:div {:class "page-header"}
       [:h1 "Queues"]]
      [:div {:class "table-wrapper"}
       [:table {:class "data-table"}
        [:thead
         [:tr
          [:th "Name"] [:th "Status"] [:th "Created At"] [:th "Actions"]]]
        [:tbody
         (if (seq queues)
           (map queue-row queues)
           [:tr [:td {:colspan 4 :class "empty"} "No queues found."]])]]]))))
