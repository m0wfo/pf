(ns pf.backends
  (:import [java.net InetSocketAddress]))

(def backends (ref #{(InetSocketAddress. 9292)}))

(defn app-server []
  (rand-nth (vec @backends)))

(defn decommission [backend]
  (dosync (alter backends disj backend)))

(defn backends? []
  (not (empty? @backends)))

(add-watch backends nil (fn [k r old new]
                          (println "TODO")))