(ns s-exp.drip-cli.views.jobs
  "Interactive TUI browser for drip jobs using charm.clj."
  (:require [charm.components.table :as tbl]
            [charm.components.timer :as timer]
            [charm.message :as msg]
            [charm.program :as program]
            [charm.style.core :as style]
            [clojure.string :as str]
            [s-exp.drip :as drip]
            [s-exp.drip-cli.format :as fmt]))

;; ---------------------------------------------------------------------------
;; State colours
;; ---------------------------------------------------------------------------

(def ^:private state-style
  {:available (style/style :fg style/green)
   :running (style/style :fg style/blue)
   :completed (style/style :faint true)
   :scheduled (style/style :fg style/yellow)
   :retryable (style/style :fg style/yellow)
   :discarded (style/style :fg style/red)
   :cancelled (style/style :fg style/red)
   :pending (style/style :fg style/cyan)})

(defn- state-cell [state]
  (let [s (if state (name state) "")
        st (get state-style state)]
    (if st (style/render st s) s)))

;; ---------------------------------------------------------------------------
;; Row conversion
;; ---------------------------------------------------------------------------

(def ^:private columns
  [{:title "ID" :width 8}
   {:title "KIND" :width 22}
   {:title "QUEUE" :width 14}
   {:title "STATE" :width 11}
   {:title "PRI" :width 4}
   {:title "ATT/MAX" :width 8}
   {:title "SCHEDULED" :width 20}
   {:title "CREATED" :width 20}])

(defn- job->row [job]
  [(:id job)
   (:kind job)
   (:queue job)
   (state-cell (:state job))
   (:priority job)
   (str (:attempt job) "/" (:max-attempts job))
   (fmt/fmt-instant (:scheduled-at job))
   (fmt/fmt-instant (:created-at job))])

;; ---------------------------------------------------------------------------
;; Detail view helpers
;; ---------------------------------------------------------------------------

(defn- label [text]
  (style/render (style/style :bold true :width 18) text))

(defn- section-header [text]
  (style/render (style/style :bold true :underline true :fg style/white) text))

(defn- kv-line [k v]
  (style/join-horizontal :top (label k) (str v)))

(defn- fmt-map [m]
  (cond
    (map? m)
    (str/join "\n" (map (fn [[k v]] (str "  " (if (keyword? k) (name k) (str k)) ": " v)) m))
    (sequential? m)
    (str/join "\n" (map-indexed (fn [i v] (str "  [" i "] " v)) m))
    :else
    (str m)))

(defn- error-block [idx {:keys [error at attempt trace]}]
  (str/join "\n"
            (remove nil?
                    [(style/render (style/style :fg style/red :bold true)
                                   (str "  [" (inc idx) "] attempt " attempt
                                        (when at (str "  " at))))
                     (when error (str "  error: " error))
                     (when trace
                       (let [lines (str/split-lines trace)
                             preview (take 5 lines)]
                         (str/join "\n" (map #(str "    " %) preview))))])))

(defn- detail-lines [job]
  (let [row (fn [k s] (kv-line k s))
        blank ""]
    (remove nil?
            (concat
             [(section-header "IDENTITY")
              (row "id" (:id job))
              (row "kind" (:kind job))
              (row "queue" (:queue job))
              blank
              (section-header "STATE")
              (kv-line "state" (state-cell (:state job)))
              (row "priority" (:priority job))
              (row "attempt" (str (:attempt job) " / " (:max-attempts job)))
              blank
              (section-header "TIMESTAMPS")]
             (keep (fn [[k inst]]
                     (when inst (row k (fmt/fmt-instant inst))))
                   [["created-at" (:created-at job)]
                    ["scheduled-at" (:scheduled-at job)]
                    ["attempted-at" (:attempted-at job)]
                    ["finalized-at" (:finalized-at job)]])
             (when-let [tags (seq (:tags job))]
               [blank (section-header "TAGS") (str "  " (str/join ", " tags))])
             (when-let [args (:args job)]
               [blank (section-header "ARGS") (fmt-map args)])
             (when-let [meta (:metadata job)]
               [blank (section-header "METADATA") (fmt-map meta)])
             (when-let [errors (seq (:errors job))]
               (into [blank (section-header "ERRORS")]
                     (map-indexed error-block errors)))))))

;; ---------------------------------------------------------------------------
;; Init / update / view
;; ---------------------------------------------------------------------------

(def ^:private default-refresh-ms 5000)
(def ^:private page-size 20)

(defn- make-table [jobs]
  (tbl/table columns (mapv job->row jobs) :cursor 0 :height page-size
             :keys {:cursor-up ["up" "k" "ctrl+p"]
                    :cursor-down ["down" "j" "ctrl+n"]}))

(defn- make-refresh-timer [interval-ms]
  (timer/timer :timeout interval-ms :interval interval-ms))

(defn- status-bar [n page has-next? auto-refresh?]
  (str n " jobs  |  page " (inc page)
       (when has-next? "+")
       "  |  q quit  |  enter detail  |  r refresh"
       (if has-next? "  |  > next" "")
       (when (pos? page) "  |  < prev")
       (if auto-refresh? "  |  a pause auto-refresh" "  |  a resume auto-refresh")))

(defn- fetch-page [client base-opts after]
  (let [opts (cond-> (assoc base-opts :limit (inc page-size))
               after (assoc :after after))]
    (drip/list-jobs client opts)))

(defn- init [client opts refresh-ms]
  (fn []
    (let [raw (fetch-page client opts nil)
          has-next? (> (count raw) page-size)
          jobs (vec (take page-size raw))
          tmr (make-refresh-timer refresh-ms)
          [tmr cmd] (timer/timer-init tmr)]
      [{:mode :list
        :table (make-table jobs)
        :jobs jobs
        :auto-refresh true
        :refresh-ms refresh-ms
        :timer tmr
        :page 0
        :page-stack []
        :has-next? has-next?
        :status (status-bar (count jobs) 0 has-next? true)
        :message nil}
       cmd])))

(defn- do-refresh [client opts state]
  (let [after (peek (:page-stack state))
        raw (fetch-page client opts after)
        has-next? (> (count raw) page-size)
        jobs (vec (take page-size raw))
        [new-tmr cmd] (timer/reset (:timer state) (:refresh-ms state))]
    [(assoc state
            :jobs jobs
            :table (make-table jobs)
            :timer new-tmr
            :has-next? has-next?
            :status (status-bar (count jobs) (:page state) has-next? (:auto-refresh state)))
     cmd]))

(defn- go-next-page [client opts state]
  (let [jobs (:jobs state)
        after (:id (last jobs))
        raw (fetch-page client opts after)
        has-next? (> (count raw) page-size)
        new-jobs (vec (take page-size raw))]
    (if (seq new-jobs)
      (let [new-page (inc (:page state))
            new-stack (conj (:page-stack state) after)]
        [(assoc state
                :jobs new-jobs
                :table (make-table new-jobs)
                :page new-page
                :page-stack new-stack
                :has-next? has-next?
                :status (status-bar (count new-jobs) new-page has-next? (:auto-refresh state)))
         nil])
      [state nil])))

(defn- go-prev-page [client opts state]
  (let [stack (:page-stack state)]
    (if (empty? stack)
      [state nil]
      (let [new-stack (pop stack)
            after (peek new-stack)
            raw (fetch-page client opts after)
            has-next? (> (count raw) page-size)
            new-jobs (vec (take page-size raw))
            new-page (dec (:page state))]
        [(assoc state
                :jobs new-jobs
                :table (make-table new-jobs)
                :page new-page
                :page-stack new-stack
                :has-next? has-next?
                :status (status-bar (count new-jobs) new-page has-next? (:auto-refresh state)))
         nil]))))

(defn- update-fn [client opts]
  (fn [state msg]
    (case (:mode state)
      :list
      (cond
        (msg/key-match? msg "q")
        [state program/quit-cmd]

        (msg/key-match? msg "r")
        (do-refresh client opts state)

        (or (msg/key-match? msg ">") (msg/key-match? msg "."))
        (if (:has-next? state)
          (go-next-page client opts state)
          [state nil])

        (or (msg/key-match? msg "<") (msg/key-match? msg ","))
        (go-prev-page client opts state)

        (msg/key-match? msg "a")
        (let [auto? (not (:auto-refresh state))]
          (if auto?
            (let [[new-tmr cmd] (timer/reset (:timer state) (:refresh-ms state))]
              [(assoc state :auto-refresh true :timer new-tmr
                      :status (status-bar (count (:jobs state)) (:page state) (:has-next? state) true))
               cmd])
            (let [[new-tmr _] (timer/stop (:timer state))]
              [(assoc state :auto-refresh false :timer new-tmr
                      :status (status-bar (count (:jobs state)) (:page state) (:has-next? state) false))
               nil])))

        (or (msg/key-match? msg "enter") (msg/key-match? msg "return"))
        (let [idx (tbl/table-cursor (:table state))
              job (get (:jobs state) idx)]
          (if job
            [(assoc state :mode :detail :selected-job job) nil]
            [state nil]))

        (timer/tick-msg? msg)
        (if (:auto-refresh state)
          (do-refresh client opts state)
          [state nil])

        :else
        (let [[new-tbl cmd] (tbl/table-update (:table state) msg)]
          [(assoc state :table new-tbl) cmd]))

      :detail
      (cond
        (or (msg/key-match? msg "q") (msg/key-match? msg "esc") (msg/key-match? msg "backspace"))
        [(assoc state :mode :list) nil]

        :else
        [state nil]))))

(defn- header-bar [text]
  (style/render (style/style :bold true :bg style/blue :fg style/white
                             :width 120 :padding [0 1])
                text))

(defn- view-fn [state]
  (case (:mode state)
    :list
    (str (header-bar "drip-cli  —  jobs") "\n"
         (tbl/table-view (:table state)) "\n\n"
         (:status state))

    :detail
    (let [job (:selected-job state)]
      (str (header-bar (str "job " (:id job) "  —  " (name (:state job)) "  —  esc/q back")) "\n"
           (str/join "\n" (detail-lines job))))))

;; ---------------------------------------------------------------------------
;; Entry point
;; ---------------------------------------------------------------------------

(defn run [client opts]
  (let [refresh-ms (or (some-> (:refresh opts) parse-long (* 1000))
                       default-refresh-ms)
        list-opts (dissoc opts :refresh)]
    (program/run {:init (init client list-opts refresh-ms)
                  :update (update-fn client list-opts)
                  :view view-fn
                  :alt-screen true
                  :hide-cursor false})))
