(ns ptt-analysis.endpoint
  (:require [org.httpkit.client :as http]
            [clj-time.core :as t]
            [taoensso.timbre :as timbre
             :refer (log info warn error trace)])
  (:import (java.io InputStream)))

(def ^:dynamic base-path "")

(defn ptt-endpoint
  ([path]
   (ptt-endpoint path {})
   )
  ([path options]
   (ptt-endpoint path options nil)
    )
  ([path options callback]
   (ptt-endpoint path options callback  (promise) 0)
    )
  ([path options callback  promise fails]
   (println (str base-path "https://www.ptt.cc" path))
   (http/get (str base-path "https://www.ptt.cc" path)
             {:headers (merge (:headers options)
                              {"Accept-Encoding" "gzip, deflate"
                               "cookie" "over18=1;"})
              :timeout 10000             ; ms
              :user-agent "I-am-an-ptt-crawler"
              :insecure? true}
             (fn [{:keys [status body] :as ret}]
               (let [; slurp the body if needed
                     ret (cond-> ret
                                 (and body (isa? (class body) InputStream))
                                 (assoc :body (slurp body)))]
                 (cond
                   ; for server error or any other kind of exception (except for 404)
                   ; retry with expotential backoff
                   (or (#{503 500 502} status) (:error ret) (not status))
                   (let [wait (min (* 1000 60 20) (* (Math/pow 1.5 fails) 500))]
                     (Thread/sleep wait)
                     (ptt-endpoint path options callback  promise (inc fails))
                     )
                   ; success! update performance metrics and return the response
                   :default
                   (do
                     (if callback (callback ret))
                     (deliver promise ret))
                   )
                 )
               ))
    promise
    )
  )