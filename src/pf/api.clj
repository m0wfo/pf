(ns pf.api
  (:import [java.nio.file Files Paths]
           [java.net URI])
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
  (let [merged (merge base-headers options)
        lines (map #(str (name (first %)) ": " (last %)) merged)
        joined (interpose "\r\n" lines)]
    (apply str (apply str joined) "\r\n\r\n")))

(defn dispatch [keys params]
  (let [method (first keys)
        path (into [] (remove #(= % "")
                              (split-string (last keys) #"/")))]
    
    (match [method path]
           ["GET" []] (index-page)
           ["GET" ["listeners" _ "park"]] (respond 200 "wibble" "text/plain")
           ["HEAD" _] (not-implemented)
           :else (not-found))))

(defn render-file
  ([name] (render-file name 200))
  ([name code]
     (let [resource (.getFile (clojure.java.io/resource name))
        path (Paths/get (URI. (apply str "file://" resource)))
        data (apply str (Files/readAllLines path pf.core/charset))]
    (respond code data "text/html"))))

(defn index-page []
  (render-file "index.html"))

(defn not-found []
  (render-file "404.html" 404))

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
  (letfn [(extras [c t]
            (if-not (nil? c)
              {:Content-type t :Content-length (count c)}))]
    (let [http-version "HTTP/1.1"
          desc (get response-codes code)
          topline (apply str http-version " " code " " desc "\r\n")
          hdrs (headers (extras content type))
          response (apply str topline hdrs content)]
      (pf.core/to-bb response))))

(defn handle [request]
  (read-channel request (fn [br buffer]
                          (let [data (pf.core/to-string buffer)
                                response (process data)]
                            (write-channel request
                                           #(. (request :channel) close)
                                           response)))))

(def api (start-server 1337 #(handle %)))
