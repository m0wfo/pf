(ns pf.api
  (:use [pf.core]))

(defn handler [request]
  (read-channel request (fn [br buffer]
                                  )))

(def api (pf.core/start-server 1337 #(handler %)))