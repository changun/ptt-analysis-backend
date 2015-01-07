(ns ptt-analysis.server

  (:require [clojure.core.async :refer [chan <! go put! <!! >!]]
            [ring.middleware.cors :refer [wrap-cors]]
            [ptt-analysis.more-like-this :as more-like-this])
  (:use org.httpkit.server)
  (:use [ring.middleware.params         :only [wrap-params]]
        [ring.middleware.json :only [wrap-json-response]]
        [ptt-analysis.async]
        )
  )
(defonce server (atom nil))



(defn handler [{:keys [uri] :as req}]
  (cond
    (= uri "/search")
      (more-like-this/search-handler req)
    (= uri "/health")
      (with-channel req channel
        (async-get "http://localhost:8983/solr/collection1/admin/ping?wt=json" {}
                   #(send! channel {:status (or (:status %) 503)
                                    :body (or (:body %) "") }))
      )
    )
  ) ;; all other, return 404

(defn stop-server []
  (when-not (nil? @server)
    ;; graceful shutdown: wait 100ms for existing requests to be finished
    ;; :timeout is optional, when no timeout, stop immediately
    (@server :timeout 100)
    (reset! server nil)))



(defn start  [port]
  (stop-server)
  (reset! server (run-server #'handler {:port port}))
  )



