(ns pf.core
  (:import [java.nio.channels
            AsynchronousChannelGroup
            AsynchronousSocketChannel
            AsynchronousServerSocketChannel
            CompletionHandler]
           [java.nio ByteBuffer]
           [java.nio.charset Charset]
           [java.net InetSocketAddress]
           [java.util.concurrent Executors])
  (:use [pf.logging]))

(defprotocol ChannelOps
  "Channel manipulation interface."
  (close-channel [this])
  (read-channel [this cb] [this cb buffer])
  (write-channel [this cb buffer])
  (relay [source target]))

(defprotocol Acceptor
  "Server manipulation interface."
  (start [this])
  (park [this cb])
  (unpark [this])
  (parked? [this])
  (stop [this]))

(defmacro callback [& body]
  `(proxy [CompletionHandler] []
     ~@body))

(def charset (Charset/forName "UTF-8"))

(defn new-uid {:private true} []
  (. (java.util.UUID/randomUUID) toString))

(defn to-string [^ByteBuffer buffer]
  (.toString (.decode ^Charset charset buffer)))

(defn to-bb [string]
  (.encode ^Charset charset ^String string))

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

(defrecord Channel [channel counter]
  ChannelOps
  (close-channel [this]
    (. (:channel this) close)
    (dosync (commute (:counter this) dec)))

  (read-channel [this cb] (let [bb (ByteBuffer/allocate 512)]
                  (. ^ByteBuffer bb clear)
                  (read-channel this cb bb)))
  
  (read-channel [this cb buffer] (. ^AsynchronousSocketChannel (:channel this) read
                                    ^ByteBuffer buffer nil
                                    ^CompletionHandler (callback
                                                        (completed [bytes-read att]
                                                                   (if (<= 0 bytes-read)
                                                                     (do
                                                                       (. ^ByteBuffer buffer flip)
                                                                       (cb bytes-read buffer))
                                                                     (close-channel this)))
                                                        (failed [reason att]
                                                                (close-channel this)))))

  (write-channel [this cb buffer]
  "Write to a channel, executing a callback when the contents
   of a buffer has been written."
    (. ^AsynchronousSocketChannel (:channel this) write
       ^ByteBuffer buffer nil
       ^CompletionHandler (callback
                           (completed [x y]
                                      (. ^ByteBuffer buffer clear)
                                      (cb)))))

  (relay [source target]
    "Forward the contents of the first channel into a second one."
     (read-channel source (fn [read buffer]
                            (write-channel target
                                           (fn [] (relay source target)) buffer)))))

(defn new-channel [] (Channel. (AsynchronousSocketChannel/open) nil))

(defn clear-backlog {:private true} [backlog handler]
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

(defrecord Server [port handler up parked active channel group service]
  Acceptor
  (start [this]
    (let [backlog (ref '())
          server (:channel this)
          up (:up this)
          parked (:parked this)
          active (:active this)
          handler (:handler this)]

    (doto server
      (.bind (InetSocketAddress. (:port this)))
      (.accept nil (callback
                    (completed [ch attr]
                               (if (true? @up)
                                 (do
                                   (. server accept nil this)
                                   
                                   (if (true? @parked)
                                     (dosync
                                      (alter backlog conj
                                             (agent (Channel. ch nil))))
                                     (do
                                       (dosync (commute active inc))
                                       (handler (Channel. ch active))))))))))

    (add-watch up nil (fn [k r old now]
                        (if (false? now)
                          (letfn [(halt [] (kill-server server (:group this) (:service this)))]
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
                              (clear-backlog backlog handler))))))

  (park [this cb]
    "Wait for current requests to finish, and hold new ones
     in a backlog. When the server is unparked the backlog of
     requests will be passed through to the backends. If a
     callback is supplied, it will be executed when all active
     requests have closed."
    (if (compare-and-set! (:parked this) false true)
      (if-not (nil? cb)
        (letfn [(await-clients [server cb]
                  (future
                    (while (not (= 0 (server :active)))
                      (Thread/sleep 100))
                    (cb)))]
          (await-clients this cb)))))

  (unpark [this]
    "Passes new requests straight through to the backends and
     clears the request backlog that accumulated while the server
     was parked."
    (compare-and-set! (:parked this) true false))

  (parked? [this]
    "True if the given server is parked."
    (let [active (:active this)
          parked (:parked this)]
      (and (= @active 0) (true? @parked))))
  
  (stop [this]
    "Gracefully shutdown a server, waiting for all
    incoming connections to close. Returns immediately."
    (compare-and-set! (:up this) true false)))

(defn create-server [port handler]
  (let [factory (Executors/defaultThreadFactory)
        service (Executors/newCachedThreadPool factory)
        group (AsynchronousChannelGroup/withCachedThreadPool service 1)
        channel (AsynchronousServerSocketChannel/open group)
        up (atom true)
        parked (atom false)        
        active (ref (int 0))]
    (Server. port handler up parked active channel group service)))

