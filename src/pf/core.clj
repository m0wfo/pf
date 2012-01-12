(ns pf.core
  (:import [java.nio.channels
            AsynchronousChannelGroup
            AsynchronousSocketChannel
            AsynchronousServerSocketChannel
            CompletionHandler]
           [java.nio ByteBuffer]
           [java.net InetSocketAddress]
           [java.util.concurrent Executors])
  (:gen-class))

(defmacro callback [& body]
  `(proxy [CompletionHandler] []
     ~@body))

(defn relay [channel target buffer]
  (. channel read buffer nil (callback
                               (completed [br attr]
                                 (if (< br 0)
                                   (. channel close)
                                   (do (. buffer flip)
                                       (. target write buffer nil (callback
                                          (completed [x y]
                                                     (. buffer clear)
                                                     (relay channel target buffer))))))))))

(defn handle [channel-in]
  (let [buffer-in (ByteBuffer/allocate 256)
        buffer-out (ByteBuffer/allocate 256)
        target (AsynchronousSocketChannel/open)]
    (println "Incoming from " (. channel-in getRemoteAddress))
    (. target connect (InetSocketAddress. 9292) nil (callback
                                                      (completed [x y]
                                                        (. buffer-in clear)
                                                        (. buffer-out clear)
                                                        (relay channel-in target buffer-in)
                                                        (relay target channel-in buffer-out))))))

(defn start-server [port]
  (let [factory (Executors/defaultThreadFactory)
        service (Executors/newCachedThreadPool factory)
        group (AsynchronousChannelGroup/withCachedThreadPool service 1)
        server (AsynchronousServerSocketChannel/open group)]
    (doto server
      (.bind (InetSocketAddress. port))
      (.accept nil (callback
                     (completed [ch attr]
                       (. server accept nil this)
                       (handle ch)))))))

(defn -main [& args]
  (start-server 8080)
  (read-line))
