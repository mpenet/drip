(ns drip-ui.datastar
  "Datastar SSE event helpers."
  (:require [jsonista.core :as json]))

(defn- patch-element-lines
  "Returns a seq of data-line strings for a single element patch."
  [element {:keys [selector mode use-view-transition]}]
  (cond-> []
    selector (conj (str "selector " (name selector)))
    mode (conj (str "mode " (name mode)))
    use-view-transition (conj (str "useViewTransition " (boolean use-view-transition)))
    :then (conj (str "elements " element))))

(defn patch-elements
  "Returns an SSE message map for Datastar's datastar-patch-elements event.
   `elements` is a seq of HTML fragment strings.
   opts: :selector, :mode, :use-view-transition."
  [elements & {:keys [selector mode use-view-transition] :as opts}]
  {:event "datastar-patch-elements"
   :data (into [] (mapcat #(patch-element-lines % opts)) elements)})

(defn patch-signals
  "Returns an SSE message map for Datastar's datastar-patch-signals event.
   `signals` is a seq of maps to merge into Datastar's signal store."
  [signals & {:keys [only-if-missing]}]
  {:event "datastar-patch-signals"
   :data (cond-> []
           only-if-missing
           (conj (str "onlyIfMissing " only-if-missing))
           :then
           (into (mapv #(str "signals " (json/write-value-as-string %)) signals)))})
