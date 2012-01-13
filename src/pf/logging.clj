(ns pf.logging)

(def logger (agent (System/out)))

(defn log [& txt]
  (let [msg (str txt)]
    (letfn [(write [writer str]
            (. writer println str)
              writer)]
      (send-off logger write msg))))

