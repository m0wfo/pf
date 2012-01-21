(ns pf.api
  (:use [pf.core]
        [clojure.core.match :only [match]])
  (:require [clojure.string :only split]))

(def base-headers {:Server "pf 0.0.1 (Bumcakes)" :Connection "close"})

(def response-codes {200 "OK"
                       202 "Accepted"
                       404 "Not Found"
                       501 "Not Implemented"})

(defn split-string [str predicate]
  (clojure.string/split str predicate))

(defn headers [options]
  (let [lines (map #(str (name (first %)) ": " (last %)) options)
        joined (interpose "\r\n" lines)]
    (apply str (apply str joined) "\r\n\r\n")))

(defn dispatch [keys params]
  (let [method (first keys)
        path (into [] (remove #(= % "")
                              (split-string (last keys) #"/")))]
    
    (match [method path]
           ["GET" []] (println "index page")
           ["GET" ["listeners"]] (println "foo")
           ["HEAD" _] (not-implemented)
           :else (println "default route"))))

(defn not-implemented []
  (respond 501))

(defn parse-params [in]
  (let [pairs (split-string in #"&")
        keys-values (map (fn [i]
                           (let [k-v (split-string i #"=")]
                             [(keyword (first k-v))
                              (last k-v)])) pairs)]
    (into {} keys-values)))

(defn process [in]
  (let [header-body (split-string in  #"\r\n\r\n")
        lines (split-string (first header-body) #"\r\n")
        cmd (split-string (first lines) #"\s+")
        route (subvec cmd 0 2)]
    (if (= 2 (count header-body))
      (dispatch route (parse-params (last header-body)))
      (dispatch route nil))))

(defn respond [code & [content type]]
  (let [http-version "HTTP/1.1"
        desc (get response-codes code)
        topline (apply str http-version " " code " " desc "\r\n")
        headers (apply str topline (headers base-headers))]
    (pf.core/to-bb headers)))

(defn handle [request]
  (read-channel request (fn [br buffer]
                          (let [data (pf.core/to-string buffer)
                                response (process data)]
                            (write-channel request
                                           #(. (request :channel) close)
                                           response)))))

(def api (start-server 1337 #(handle %)))
