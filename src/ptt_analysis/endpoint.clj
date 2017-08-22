(ns ptt-analysis.endpoint
  (:require [clj-http.client :as http]
            [clj-time.core :as t]
            [taoensso.timbre :as timbre
             :refer (log info warn error trace)])
  (:import (java.io InputStream)
           (org.littleshoot.proxy.impl DefaultHttpProxyServer)))

(def ^:dynamic base-path "")

(def proxy-server
  (when-not *compile-files*
    (.start (DefaultHttpProxyServer/bootstrap))
    ))

(defn ptt-endpoint
  ([path]
   (ptt-endpoint path {})
   )
  ([path options]
   (ptt-endpoint path options 0)
    )
  ([path options  fails]
   (let [{:keys [status body] :as ret}
         (http/get (str base-path "https://www.ptt.cc" path)
                      (cond->
                        {:headers (merge (:headers options)
                                         {"Accept-Encoding" "gzip, deflate"
                                          "cookie" "over18=1;"})
                         :timeout 10000             ; ms
                         :user-agent "I-am-an-ptt-crawler"
                         :insecure? true
                         :client-params {:cookie-policy (constantly nil)}
                         :throw-exceptions false
                         }
                        (= base-path "")
                        ; use proxy for local request
                        (assoc :proxy-host "127.0.0.1" :proxy-port 8080))


                       )
         ret (cond-> ret
                 (and body (isa? (class body) InputStream))
                 (assoc :body (slurp body)))
         ]
     (cond
       ; for server error or any other kind of exception (except for 404)
       ; retry with expotential backoff
       (or (#{503 500 502} status) (:error ret) (not status))
       (let [wait (min (* 1000 60 20) (* (Math/pow 1.5 fails) 500))]
         (Thread/sleep wait)
         (recur path options (inc fails))
         )
       ; success! update performance metrics and return the response
       :default
       ret
       )
     )
    )
  )