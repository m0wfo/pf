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

(defn relay [channel target buffer]
  (. channel read buffer nil (proxy [CompletionHandler] []
                               (completed [br attr]
                                 (if (< br 0)
                                   (. channel close)
                                   (do (. buffer flip)
                                       (. target write buffer nil (proxy [CompletionHandler] []
                                          (completed [x y]
                                                     (. buffer clear)
                                                     (relay channel target buffer))))))))))

(defn handle [channel-in]
  (let [buffer-in (ByteBuffer/allocate 256)
        buffer-out (ByteBuffer/allocate 256)
        target (AsynchronousSocketChannel/open)]
    (. target connect (InetSocketAddress. 9292) nil (proxy [CompletionHandler] []
                                                      (completed [x y]
                                                        (. buffer-in clear)
                                                        (. buffer-out clear)
                                                        (trampoline relay channel-in target buffer-in)
                                                        (trampoline relay target channel-in buffer-out))))))

(defn start-server []
  (let [factory (Executors/defaultThreadFactory)
        service (Executors/newCachedThreadPool factory)
        group (AsynchronousChannelGroup/withCachedThreadPool service 1)
        server (AsynchronousServerSocketChannel/open group)]
    (doto server
      (.bind (InetSocketAddress. 8080))
      (.accept nil (proxy [CompletionHandler] []
                     (completed [ch attr]
                       (. server accept nil this)
                       (handle ch)))))))

(defn -main [& args]
  (println "starting server")
  (start-server)
  (read-line))
