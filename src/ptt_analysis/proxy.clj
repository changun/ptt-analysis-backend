(ns ptt-analysis.proxy
  (:import (java.net SocketException)
           (com.squareup.okhttp Request$Builder OkHttpClient)
           (javax.net.ssl X509TrustManager SSLContext)
           (java.security SecureRandom)
           (java.security.cert X509Certificate))
  (:use org.httpkit.server)
  )
(def client (let [ssl (SSLContext/getInstance "SSL")]
              (.init ssl nil (into-array (list (proxy [X509TrustManager] []
                                                 (getAcceptedIssuers [] (make-array X509Certificate 0))
                                                 (checkClientTrusted [ chain auth-type]
                                                   )
                                                 (checkServerTrusted [ chain auth-type]
                                                   ))))
                     (SecureRandom.))
              (.setSslSocketFactory (OkHttpClient.) (.getSocketFactory ssl) )))

(defonce server (atom nil))

(defn handler [{:keys [uri] :as req}]
  (with-channel req channel
                (try (let [res (.execute (.newCall client (-> (Request$Builder.)
                                                              (.url  (str "https://www.ptt.cc/" uri))
                                                              (.header "cookie" "over18=1;")
                                                              (.build)
                                                              )
                                                   ))]
                       (send! channel {:status (.code res) :body (-> res
                                                                     (.body)
                                                                     (.string))})
                       )

                     (catch Exception e (send! channel {:status 500 :body (str e)})))
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
