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


(defstruct acceptor :channel :up :parked :active)
(defstruct counted-channel :channel :counter)

(defmacro callback [& body]
  `(proxy [CompletionHandler] []
     ~@body))

(defn new-uid {:private true} []
  (. (java.util.UUID/randomUUID) toString))

(defn test-or-watch [value expected action]
  "Compares a concurrency primitive's value with an expected
   one. If the two values are equal execute action immediately.
   If not, watch the primitive until the equality is satisfied,
   then perform action."
  (if (= @value expected)
     (action)
     (let [uid (new-uid)]
       (add-watch value uid (fn [k v old-val new-val]
                                (if (= new-val expected)
                                  (do
                                    (remove-watch value uid)
                                    (action))))))))

(defn new-channel [] (struct counted-channel (AsynchronousSocketChannel/open) nil))

(defn read-channel
  ([channel cb] (let [bb (ByteBuffer/allocate 512)]
                  (. bb clear)
                  (read-channel channel buffer cb)))
  ([channel buffer cb] (. (channel :channel) read buffer nil (callback
                                         (completed [bytes-read att]
                                                    (if (<= 0 bytes-read)
                                                      (do
                                                        (. buffer flip)
                                                        (cb bytes-read att))
                                                      (do
                                                        (. (channel :channel) close)
                                                        (if-not (nil? (channel :counter))
                                                          (dosync (commute (channel :counter) dec))))))
                                         (failed [reason att]
                                                 (println "bumcakes"))))))

(defn write-channel [channel buffer cb]
  "Write to a channel, executing a callback when the contents
   of a buffer has been written."
  (. (channel :channel) write buffer nil (callback
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

(defn clear-backlog [backlog handler]
  "Passes a handler to each request wrapped in an agent
  and accumulated in a backlog, iteratively removing it."
  (doseq [request @backlog]
    (dosync
     (send-off request handler)
     (alter backlog pop))))

(defn kill-server {:private true} [server group service]
  (. server close)
  (. group shutdown)
  (. service shutdownNow))

(defn start-server
  ([port] (start-server port #(. % close)))
  
  ([port handler] (let [factory (Executors/defaultThreadFactory)
                        service (Executors/newCachedThreadPool factory)
                        group (AsynchronousChannelGroup/withCachedThreadPool service 1)
                        server (AsynchronousServerSocketChannel/open group)
                        up (atom true)
                        parked (atom false)
                        backlog (ref '())
                        active (ref 0)]

    (doto server
      (.bind (InetSocketAddress. port))
      (.accept nil (callback
                    (completed [ch attr]
                               (if (true? @up)
                                 (do
                                   (. server accept nil this)
                                   
                                   (if (true? @parked)
                                     (dosync
                                      (alter backlog conj
                                             (agent (struct counted-channel ch nil))))
                                     (do
                                       (dosync (commute active inc))
                                       (handler (struct counted-channel ch active))))))))))

    (add-watch up nil (fn [k r old now]
                        (if (false? now)
                          (letfn [(halt [] (kill-server server group service))]
                            ; If we initiate shutdown while server is
                            ; parked, a deadlock will occur if the backlog is
                            ; non-empty. Since we're no longer taking requests
                            ; it's safe to unpark and clear any stragglers
                            (compare-and-set! parked true false)
                            
                            (if (= @active 0)
                              (halt)
                              (add-watch active nil (fn [ak ar o n]
                                                      (if (= n 0)
                                                        (halt)))))))))

    (add-watch parked nil (fn [k r old now]
                            (if (false? now)
                              (clear-backlog backlog handler))))
    
    (struct acceptor server up parked active))))

(defn server-parked? [server]
  "True if the given server is parked."
  (let [active (server :active)
        parked (server :parked)]
    (and (= @active 0) (true? @parked))))

(defn await-clients {:private true} [server cb]
  (future
    (while (not (= 0 (server :active)))
      (Thread/sleep 100))
    (cb)))

(defn park-server [server & [cb]]
  "Wait for current requests to finish, and hold new ones
   in a backlog. When the server is unparked the backlog of
   requests will be passed through to the backends. If a
   callback is supplied, it will be executed when all active
   requests have closed."
  (if (compare-and-set! (server :parked) false true)
    (if-not (nil? cb) (await-clients server cb))))

(defn unpark-server [server]
  "Passes new requests straight through to the backends and
   clears the request backlog that accumulated while the server
   was parked."
  (compare-and-set! (server :parked) true false))

(defn stop-server [server]
  "Gracefully shutdown a server, waiting for all
  incoming connections to close. Returns immediately."
  (compare-and-set! (server :up) true false))

