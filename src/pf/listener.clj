(ns pf.listener
  (:use [pf.core]
        [pf.backends]
        [pf.logging]))

(defrecord Listener [name acceptor])

(defn handle [in]
  (let [out (new-channel)
        backend (app-server)]
    (. (out :channel) connect backend nil (callback
                                (completed [x y]
                                           ; Patch
                                           ; the two
                                           ; channels together
                                           (relay in out)
                                           (relay out in))

                                (failed [reason att]
                                        (log reason)
                                        (decommission backend)
                                        (if (backends?)
                                          (handle in)))))))

(defn new-listener [name port]
  (let [acceptor (pf.core/start-server port)]
    (Listener. name acceptor)))

(defn s [] (start-server 8080 #(handle %)))
