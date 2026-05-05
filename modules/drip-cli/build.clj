(ns build
  (:require [clojure.tools.build.api :as b]))

(def basis (delay (b/create-basis {:project "deps.edn"})))
(def uber-file "target/drip-cli.jar")
(def class-dir "target/classes")

(defn uber [_]
  (b/delete {:path "target"})
  (b/copy-dir {:src-dirs ["src"] :target-dir class-dir})
  (b/compile-clj {:basis @basis :src-dirs ["src"] :class-dir class-dir})
  (b/uber {:class-dir class-dir
           :uber-file uber-file
           :basis @basis
           :main 's-exp.drip-cli}))
