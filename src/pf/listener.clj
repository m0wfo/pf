(ns pf.listener
  (:use [pf.core]
        [pf.backends]
        [pf.logging]))

(defprotocol Listener
  (foo [x]))

(extend-type pf.core.Server
  Listener
  (foo [x] nil))

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
  (let [listener (pf.core/create-server port #(handle %))]
    (pf.core/start listener)))

