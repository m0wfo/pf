(ns pf.core
  (:import [java.nio.channels
            AsynchronousChannelGroup
            AsynchronousSocketChannel
            AsynchronousServerSocketChannel
            CompletionHandler]
           [java.nio ByteBuffer]
           [java.net InetSocketAddress]
           [java.util.concurrent Executors])
  (:use [pf.logging]
        [pf.backends])
  (:gen-class))

(defmacro callback [& body]
  `(proxy [CompletionHandler] []
     ~@body))

(defn relay
  ([channel target] (let [bb (ByteBuffer/allocate 512)]
                     (. bb clear)
                     (relay channel target bb)))
  
  ([channel target buffer] (. channel read buffer nil (callback
                               (completed [br attr]
                                          (if (<= 0 br)
                                            (do
                                              (. buffer flip)
                                              (. target write buffer nil (callback
                                                                          (completed [x y]
                                                                                     (. buffer clear)
                                                                                     (relay channel target buffer)))))))))))

(defn handle [in]
  (let [out (AsynchronousSocketChannel/open)
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

(defn start-server
  ([port] (start-server port #(handle %)))
  
  ([port handler] (let [factory (Executors/defaultThreadFactory)
        service (Executors/newCachedThreadPool factory)
        group (AsynchronousChannelGroup/withCachedThreadPool service 1)
        server (AsynchronousServerSocketChannel/open group)]
    (doto server
      (.bind (InetSocketAddress. port))
      (.accept nil (callback
                    (completed [ch attr]
                               (. server accept nil this)
                               (handler ch)))))
    server)))

(defn stop-server [server]
  (. server close))

(def s (start-server 8080))
