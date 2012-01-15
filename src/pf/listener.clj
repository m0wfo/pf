(ns pf.listener
  (:use [pf.core]
        [pf.backends]
        [pf.logging]))

(defn handle [in]
  (let [out (new-channel)
        backend (app-server)]
    (. out connect backend nil (callback
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
                                          (handle in)
                                          (. in close)))))))

(def s (start-server 8080 #(handle %)))