(ns pf.api
  (:use [pf.core]
        [clojure.core.match :only [match]])
  (:require [clojure.string :only split]))

(def *base-headers* {:Server "pf 0.0.1 - Bumcakes"})

(defn split-string [str predicate]
  (clojure.string/split str predicate))

(defn headers [options]
  (let [lines (map #(str (name (first %)) ": " (last %)) options)
        joined (interpose "\r\n" lines)]
    (apply str (apply str joined) "\r\n\r\n")))

(defn dispatch [keys params]
  (match [keys params]
         [["GET" "/"] _] (println "index page")
         [["POST" "/register"] ({:host _ :port _ :version _} :only [:host :port :version])] (println "register")
         [["POST" "/unregister"] ({:host _ :port _} :only [:host :port])] (println "unregister")
         [["POST" "/transition"] _] (println "transition")
         [["POST" "/rollback"] _] (println "rollback")
         [["POST" "/start"] _] (println "start") 
         [["POST" "/stop"] _] (println "stop")
         [["POST" "/park"] _] (println "park")
         [["POST" "/unpark"] _] (println "unpark")
         [["HEAD" _] _] (println "head request")
         [["PUT" _] _] (println "put")
         [["DELETE" _] _] (println "delete")
           :else (println "default route")))

(defn parse-params [in]
  (let [pairs (split-string in #"&")
        keys-values (map (fn [i]
                           (let [k-v (split-string i #"=")]
                             [(keyword (first k-v))
                              (last k-v)])) pairs)]
    (into {} keys-values)))

(defn parse-headers [in]
  (let [header-body (split-string in  #"\r\n\r\n")
        lines (split-string (first header-body) #"\r\n")
        cmd (split-string (first lines) #"\s+")
        route (subvec cmd 0 2)]
    (if (= 2 (count header-body))
      (dispatch route (parse-params (last header-body)))
      (dispatch route nil))))

(defn handle [request]
  (read-channel request (fn [br buffer]
                          (let [cs (java.nio.charset.Charset/forName "UTF-8")
                                dec (. cs newDecoder)
                                cb (. dec decode buffer)]
                            (parse-headers (. cb toString)))
                          (. (request :channel) close))))

(def api (start-server 1337 #(handle %)))
