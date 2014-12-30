(ns ptt-analysis.yahoo
  (:import (org.joda.time DateTimeZone DateTime)
           (com.luhuiguo.chinese ChineseUtils))
  (:require [org.httpkit.client :as http]
            [clojure.core.async :refer [chan <! go put! <!! >!]]
            [net.cgrand.enlive-html :as html]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [taoensso.timbre :as timbre
             :refer (log info warn error trace)]
            [cheshire.core :as json]
            [taoensso.timbre.profiling :as profiling
             :refer (pspy pspy* profile defnp p p*)]
            [clj-time.coerce :as c]
            [monger.core :as mg]
            [monger.collection :as mc]
            [compojure.handler :as handler]
            [compojure.route :as route]
            [ring.middleware.cors :refer [wrap-cors]]
            [org.httpkit.client :as http])
  (:use [compojure.route :only [files not-found]]
        [compojure.handler :only [site]] ; form, query params decode; cookie; session, etc
        [compojure.core :only [defroutes GET POST DELETE ANY context]]
        org.httpkit.server)
  (:use [ring.middleware.params         :only [wrap-params]]
        [ring.middleware.json :only [wrap-json-response]]
        [ptt-analysis.async]
        )
  )
(defonce server (atom nil))

(defn async-get [& args]
  (let [ch (chan 1)]
    (apply http/get (concat  args [#(if (or (:error %) (= (:status %) 503))
                                     (put! ch (Exception. (str "Query " args "Error:" %)))
                                     (put! ch (:body %)))]))
    ch
    )
  )
(defn async-post[& args]
  (let [ch (chan 1)]
    (apply http/post (concat  args [#(if (or (:error %) (= (:status %) 503))
                                     (put! ch (Exception. (str "Query " args "Error:" %)))
                                     (put! ch (:body %)))]))
    ch
    )
  )

(defn get-more-like-this [news popularity]
  (go-try (let [body (<? (async-post  "http://localhost:8983/solr/mlt"
                                  {:query-params
                                            {"mlt.fl" "text,title"
                                             "mlt.boost" "true"
                                             "mlt.interestingTerms" "details"
                                             "fl" "id,title,score,popularity,last_modified,author,push,dislike,arrow,subject"
                                             "fq" ["last_modified:[NOW/DAY-60DAYS TO NOW/DAY+1DAY]"
                                                   (format "popularity:[%s TO *]" popularity)]

                                             "rows" "100"
                                             "mlt.qf" "text^2"
                                             "wt"  "json"
                                             }
                                   :headers {"Content-Type" "application/json"}

                                   :body (cheshire.core/generate-string
                                           (-> news (assoc :title (:title news)
                                                           :text (ChineseUtils/toTraditional
                                                                   (:content news)))))}))
            body (cheshire.core/parse-string body true)]
        (info (:interestingTerms body))
        (map (fn [doc] (let [title (:title doc)]
                         (if (and title
                                  (not (string? title))
                                  (seq title))
                           (assoc doc :title (first title))
                           doc
                           )
                         )) (:docs (:response body)))))

  )

(defn normalize [coll]
  (info coll)
  (if (seq coll)
    (if (> (count coll) 1)
      (let [avg (/ (apply + coll) (count coll))
            sd-square (/ (apply + (map #(Math/pow (double (- avg %)) 2) coll)) (count coll))]
        (info sd-square)
        (if (not= sd-square 0.0)
          (map #(/ (- % avg) (Math/sqrt sd-square)) coll)
          (repeat (count  coll) 0)
          )

        )
      [0])
    coll)
  )


(def default-response {:headers {"Content-Type" "application/json"
                                 "Access-Control-Allow-Origin" "*"
                                 "Access-Control-Allow-Methods" "POST"}
                       :status 200

                       })
(defn order-weight [{:keys [n-pop n-push n-score n-recent]}]
  (+ (* n-push 5)(* n-pop 3) (* n-score 5) (* n-recent 10))
  )
(defn best-match-weight [{:keys [n-pop n-score n-recent]}]
  (+ (* n-pop 2) (* n-score 5) (* n-recent 10))
  )
(defn search-handler [req]
  (with-channel req channel
                (let [start-time (System/nanoTime)
                      {:keys [title content href] :as news} (try (cheshire.core/parse-string (slurp (:body req)) true) (catch Exception _ nil))]
                  (if (and title content)
                    (go (try
                          (let [matches (filter #(> (:score %) 0.15) (<? (get-more-like-this news 1)))
                                matches (map (fn [pop push score recent match]
                                               (assoc match
                                                      :n-pop pop
                                                      :n-push push
                                                      :n-score score
                                                      :n-recent recent
                                                      )
                                               )
                                             (normalize (map :popularity matches))
                                             (normalize (map :push matches))
                                             (normalize (map :score matches))
                                             (normalize (map (comp
                                                               c/to-epoch
                                                               #(.withTimeAtStartOfDay ^DateTime %)
                                                               #(t/to-time-zone % (DateTimeZone/forID "Asia/Taipei"))
                                                               c/from-string :last_modified) matches))
                                             matches)
                                matches (reverse (sort-by order-weight matches))

                                best-match (first (reverse (sort-by best-match-weight
                                                                    (filter (fn [{:keys [title]}]
                                                                              (and (not= title "") (not (re-matches #"^Re:.*" title))) )
                                                                            matches))))
                                response (-> {:articles (reverse (sort-by :last_modified (take 10 matches)))}
                                             (assoc :best-match best-match)
                                             )

                                ]
                            (send! channel (merge default-response
                                                  {:body (cheshire.core/generate-string response)}))
                            (info "Served request:" req "in" (/ (- (System/nanoTime) start-time) 1e6))
                            )
                          (catch Exception e (do (error e "search error!" req)
                                                 {:status 400
                                                  :body (cheshire.core/generate-string
                                                          {:reason "Something went wrong..."}) }
                                                 ))
                          )
                        )
                    (send! channel (merge default-response
                                          {:status 400
                                           :body (cheshire.core/generate-string
                                                   {:reason "The request must contains a valid json in the body with title and content fields"}) }))
                    )
                  )

                )
  )


(defn handler [{:keys [uri] :as req}]
  (cond
    (= uri "/search")
      (search-handler req)
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



