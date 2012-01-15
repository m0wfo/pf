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

(defn new-channel [] (AsynchronousSocketChannel/open))

(defn read-channel [channel buffer cb]
  "Read from a channel, executing a callback when data is received."
  (. channel read buffer nil (callback
                                 (completed [bytes-read att]
                                            (if (<= 0 bytes-read)
                                              (do
                                                (. buffer flip)
                                                (cb bytes-read att)))))))

(defn write-channel [channel buffer cb]
  "Write to a channel, executing a callback when the contents
   of a buffer has been written."
  (. channel write buffer nil (callback
                        (completed [x y]
                                   (. buffer clear)
                                   (cb)))))

(defn relay
  "Forward the contents of the first channel into a second one."
  ([source target] (let [bb (ByteBuffer/allocate 512)]
                     (. bb clear)
                     (relay source target bb)))
  
  ([source target buffer]
     (read-channel source buffer (fn [read attr]
                                   (write-channel target buffer
                                                  (fn [] (relay source target buffer)))))))




(defn start-server
  ([port] (start-server port #(. % close)))
  
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

