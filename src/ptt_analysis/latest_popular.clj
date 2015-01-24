(ns ptt-analysis.latest-popular
  (:import (org.joda.time DateTimeZone DateTime)
           (com.luhuiguo.chinese ChineseUtils))
  (:require [cheshire.core :as json]
            [ptt-analysis.crawler :as crawler]
            [amazonica.aws.s3 :as s3]
            [net.cgrand.enlive-html :as html]
            [ring.util.codec :as codec])
  (:require
    [clj-time.core :as t]
    [taoensso.timbre :as timbre
     :refer (log info warn error trace)]
    [cheshire.core :as json]
    [taoensso.timbre.profiling :as profiling
     :refer (pspy pspy* profile defnp p p*)]
    [clj-time.coerce :as c]
    [clojure.core.async :refer [chan <! go put! <!! >!]])
  (:use org.httpkit.server
        ptt-analysis.async))

(defn str->push-time [post-dt push-date]

  (try
    (let [[_ m d h mi] (re-find #"(\d\d)/(\d\d) (\d+):(\d+)?"  push-date)

          ]
      (DateTime. (t/year post-dt)
                 (Integer/parseInt m)
                 (Integer/parseInt d)
                 (Integer/parseInt h)
                 (Integer/parseInt mi)
                 (DateTimeZone/forID "Asia/Taipei")
                 )
      )
    (catch Exception e (do (error e push-date)
                           nil)))
  )


(defn download-from-s3 [{:keys [id]}]

  (let [[board id] (clojure.string/split id #":")
        key (str "posts/" board "/" id ".html")]
    (-> (s3/get-object ptt-analysis.crawler/cred
                       :bucket-name "ptt.rocks"
                       :key key)
        (:input-stream)
        (slurp))
    )
  )
(defn get-latest-docs [terms popularity hours-before]
  (go-try (let [body (<? (async-get  "http://localhost:8983/solr/select"
                                      {:query-params
                                                {"q" terms
                                                 "fq"
                                                 [(format "last_modified:[NOW-%dHOUR TO NOW]" hours-before)
                                                  (format "popularity:[%d TO *]" popularity)]

                                                 "rows" 500
                                                 "fl" "title,score,push,popularity,dislike,board,id,last_modified"
                                                 "wt" "json"
                                                 }
                                       :headers {"Content-Type" "application/json"}
                                        }))
                body (json/parse-string body true)]
            (map (fn [doc] (let [title (:title doc)]
                             (if (and title
                                      (not (string? title))
                                      (seq title))
                               (assoc doc :title (first title))
                               doc
                               )
                             )) (:docs (:response body)))))

  )

(defn get-pushes [{:keys [body last_modified]}]
  (let [post-dt (t/to-time-zone (c/to-date-time last_modified) (DateTimeZone/forID "Asia/Taipei"))
        ]
      (->> body
          (ptt-analysis.crawler/parse-pushes)
          (map #(assoc % :time (str->push-time post-dt (:ip-date %))))
          (map #(dissoc % :ip-date :user))
          )
    )
  )
(def default-response {:headers {"Content-Type" "application/json"
                                 "Access-Control-Allow-Origin" "*"
                                 "Access-Control-Allow-Methods" "POST"}
                       :status 200

                       })
(defn latest-popular-handler [req]
  (with-channel req channel
      (let [params  (codec/form-decode (or (:query-string req) ""))
            terms    (ChineseUtils/toTraditional (get params "terms"))
            popularity   (try (Integer/parseInt (get params "popularity")) (catch Exception _ 80))
            hours-before (try (Integer/parseInt (get params "hours-before")) (catch Exception _ 24))]
        (if (and (> (count terms) 1) popularity hours-before)
          (go (let [search-ret (<! (get-latest-docs
                                     terms
                                     popularity
                                     hours-before))
                    search-ret (filter #(> (:score %) 0.01) search-ret)
                    search-ret (map #(assoc % :body (html/html-snippet (download-from-s3 %))) search-ret)
                    search-ret (map #(assoc %
                                            :pushes (get-pushes %)
                                            :content (crawler/parse-content (:body %))
                                            ) search-ret)
                    ]

                (send! channel (merge default-response
                                      {:status 200
                                       :body (cheshire.core/generate-string
                                               (map #(dissoc % :body) search-ret)) }))
                ))
          (send! channel (merge default-response
                                {:status 400
                                 :body {:reason "Required param is missing"} }))

          )

        )
    ))


