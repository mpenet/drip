(ns build
  (:refer-clojure :exclude [test])
  (:require [clojure.data.json :as json]
            [clojure.tools.build.api :as b]
            [clojure.tools.build.tasks.uber :as uber]))

(def lib 'drip-ui)
(def version "0.1.0-SNAPSHOT")
(def main 's-exp.drip-ui)
(def class-dir "target/classes")
(defn- uber-opts [opts]
  (assoc opts
         :lib lib :main main
         :uber-file (format "target/%s.jar" lib)
         :basis (b/create-basis {})
         :class-dir class-dir
         :src-dirs ["src" "resources"]
         :ns-compile [main]))

(defn append-json
  [{:keys [path in existing _state]}]
  {:write
   {path
    {:append false
     :string
     (json/write-str
      (concat (json/read-str (slurp existing))
              (json/read-str (#'uber/stream->string in))))}}})

(defn uber "Run the CI pipeline of tests (and build the uberjar)." [opts]
  (b/delete {:path "target"})
  (let [opts (uber-opts opts)]
    (println "\nCopying source...")
    (b/copy-dir {:src-dirs ["src" "resources"] :target-dir class-dir})
    (println (str "\nCompiling " main "..."))
    (b/compile-clj opts)
    (println "\nBuilding JAR...")

    ;; HERE is the important part
    (b/uber (assoc opts :conflict-handlers
                   {"META-INF/helidon/service.loader" :append-dedupe
                    "META-INF/helidon/feature-metadata.properties" :append-dedupe
                    "META-INF/helidon/config-metadata.json" append-json
                    "META-INF/helidon/service-registry.json" append-json})))

  opts)
