(ns ptt-analysis.endpoint
  (:require [org.httpkit.client :as http]
            [clj-time.core :as t]
            [taoensso.timbre :as timbre
             :refer (log info warn error trace)])
  (:import (java.io InputStream)))


(defn ptt-endpoint-lambda
  "Make http request to ptt.cc via AWS Lambda"
  [path & [string-hash]]

  @(http/get (str "http://localhost/lambda-proxy/proxy?url=https://www.ptt.cc" path)
             {:headers    (cond-> {"cookie" "over18=1;"}
                                  string-hash
                                  (assoc "string-hash" (str string-hash)))

              :timeout 10000             ; ms
              :user-agent "I-am-an-ptt-crawler"
              :insecure? true})
  )
(defn ptt-endpoint-local
  "Make http request to ptt.cc via distributed proxies"
  [path & [string-hash]]
  @(http/get (str "http://localhost/proxies/proxy?url=https://www.ptt.cc" path)
             {:headers (cond-> {"Accept-Encoding" "gzip, deflate"
                                "cookie" "over18=1;"}
                               string-hash
                               (assoc "string-hash" (str string-hash)))
              :basic-auth ["pttrocks" "Cens0123!"]
              :timeout 10000             ; ms
              :user-agent "I-am-an-ptt-crawler"
              :insecure? true})
  )
(def ptt-endpoint
  "The main endpoint to ptt. It optimizes the performance by using mutiple way to access ptt pages,
  including AWS Lambda function and distributed proxy servers (see http://github.com/changun/proxy).
  It handlers auto-try with expotential backoff and periodically print performance metrics.
  "
  ; performance metrics
  (let [speed-meter (atom 0)
        total-count (atom 0)
        error-count (atom 0)]
    (fn [path & [string-hash]]
      (let [start (t/now)]
        (loop [fail 0 force-local false]
          ; favor lamdba endpoint becuase it is cheaper?
          ; when force-local, always use local endpoint as some large page is only accessible via local endpoint
          (let [{:keys [status body] :as ret} (try (if (and (> (rand) 0.9 ) (not force-local))
                                                     (ptt-endpoint-lambda path string-hash)
                                                     (ptt-endpoint-local path string-hash))
                                                   (catch Exception e {:error e}))
                ; slurp the body if needed
                ret (cond-> ret
                            (and body (isa? (class body) InputStream))
                            (assoc :body (slurp body)))
                ]
            (cond
              ; Lambda return "Connection reset by peer", which occur when the page is too large for lambda to fecth.
              ; Try again with local endpoint
              (= (:error ret) "read ECONNRESET")
              (recur (inc fail) true)
              ; for server error or any other kind of exception (not including 404)
              ; retry with expotential backoff
              (or (#{503 500 502} status) (:error ret) (not status))
              (let [wait (min (* 1000 60 20) (* (Math/pow 1.5 fail) 500))]
                (if-not (= status 503)
                  (do (comment (error (:error ret) ret path "Retry in " wait "ms")))
                  )
                (swap! error-count inc)
                (Thread/sleep wait)
                (recur (inc fail) force-local))
              ; success! update performance metrics and return the response
              :default
              (do
                (swap! speed-meter + (t/in-millis (t/interval start (t/now))))
                (swap! total-count inc)
                ; print metric every 1000 requests
                (when (= 0 (mod @total-count 1000))
                  (info (format "Average response time %s Average error rate %s"
                                (float (/ @speed-meter 1000))
                                (float (/ @error-count 1000))))
                  (reset! speed-meter 0)
                  (reset! error-count 0)
                  )
                ret
                )
              )
            )

          )

        )))
  )