(ns ptt-analysis.server

  (:require [clojure.core.async :refer [chan <! go put! <!! >!]]
            [ring.middleware.cors :refer [wrap-cors]]
            [ptt-analysis.more-like-this :as more-like-this]

            [ptt-analysis.share :as share]
            [taoensso.timbre
             :refer (log info warn error trace)]
            [net.cgrand.enlive-html :as html])
  (:use org.httpkit.server)
  (:use [ring.middleware.params :only [wrap-params]]
        [ring.middleware.json :only [wrap-json-response]]
        [ring.middleware.file :only [file-request]]
        [ring.middleware.head :only [wrap-head]]

        [ptt-analysis.async]
        )
  )
(defonce server (atom nil))



(html/deftemplate
  install-page-template (clojure.java.io/resource "install.html")
  [ctxt]
  )

(defn handler [{:keys [uri] :as req}]

  (cond
    (re-matches #"/share/([^/]+)/([^/]+)"  uri)
      (let [[_ board post-id] (re-matches #"/share/([^/]+)/([^/]+)"  uri)]
        (share/share-page board post-id req)
        )
    (.startsWith uri "/install")
      {:body (apply str (install-page-template {}))}
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



