(ns ptt-analysis.solr
  (:require [cheshire.core :as json]
            [org.httpkit.client :as http]
            [clj-time.coerce :as c]
            [ptt-analysis.post :as post]
            [schema.core :as s]
            [ptt-analysis.facebook :as fb])
  (:use [ptt-analysis.schema])
  (:import (com.luhuiguo.chinese ChineseUtils)))

(defn solr-endpoint [method path & [options]]
  @(http/request (-> options
                     (assoc :method method)
                     (assoc :headers (assoc (:headers options)"Content-Type" "application/json"))
                     (assoc :url (str "http://localhost:8983/solr/collection1" path))
                     )
                 identity
                 )
  )

(s/defn ^:always-validate solr-post :- SolrPost
  [{:keys [id title content time board pushed length
           content-links push-links string-hash
           author
           ]} fb-stats]
  (let [freqs (frequencies (map :op pushed))
        pushes (or (get freqs "推") 0)
        dislike (or (get freqs "噓") 0)
        arrow (or (get freqs "→") 0)
        ]
    (merge
      fb-stats
      {:id            (str board ":" id)
       :content       (->> (cons content (map :content pushed))
                           (map #(ChineseUtils/toTraditional %)))
       :title title
       :author author


       :category      board

       :popularity    (+ pushes dislike arrow)
       :push          pushes
       :dislike       dislike
       :arrow         arrow
       :length length
       :content-links content-links
       :push-links push-links

       :isReply       (not (nil? (re-matches #"^Re.*" (or title ""))))
       :last_modified (c/to-string time)
       :string-hash   string-hash
       })
    )
  )


(defn add-post [{:keys [board id] :as post} ]
  (let [post (solr-post post (fb/stats board id))
        {:keys [status body]} (solr-endpoint :post "/update?wt=json&commitWithin=60000"
                                             {:body    (json/generate-string [post])})]
    (if-not (= 200 status)
      (throw (RuntimeException. (str status body)))
      (json/parse-string body true))
    )

  )

(defn get-post [{:keys [board id]}]
  (-> (solr-endpoint :get (format "/select?q=id:%%22%s:%s%%22&wt=json" board id)
                     )
      :body
      (json/parse-string true)
      :response
      :docs
      first)
  )


(defn optimize []
  (solr-endpoint :post "/update?optimize=true"))

(defn boards []
  (-> (solr-endpoint :get "/select?q=*%3A*&rows=0&wt=json&indent=true&facet=true&facet.field=board")
      :body
      (json/parse-string true)
      (get-in [:facet_counts :facet_fields :board])
      (->> (partition 2)
           (map (partial apply vector))
           (into {})
           )
      (keys)
      ))


(defn more-like-this [news popularity callback]
  (http/get  "http://localhost:8983/solr/mlt"
             {:query-params
                       {"mlt.fl" "text,title"
                        "mlt.boost" "true"
                        "mlt.interestingTerms" "details"
                        "fl" "id,title,score,popularity,last_modified,author,push,dislike,arrow,subject,length,fb-share-count,fb-like-count,fb-comment-count,content-links"
                        "fq" ["last_modified:[NOW/DAY-60DAYS TO NOW/DAY+1DAY]"
                              (format "popularity:[%s TO *]" popularity)]

                        "rows" "300"
                        "mlt.qf" "text^2"
                        "wt"  "json"
                        }
              :headers {"Content-Type" "application/json"}

              :body (ChineseUtils/toTraditional
                      (apply str (:content news) (repeat 5 (:title news))))}
             (fn [{:keys [body]}]
               (let [body (json/parse-string body true)]
                 (->>
                   (:response body)
                   (:docs)
                   (map (fn [doc]
                          (let [title (:title doc)]
                            (if (and title
                                     (not (string? title))
                                     (seq title))
                              (assoc doc :title (first title))
                              doc
                              )
                            )
                          ))
                   (callback)
                   )
                 ))
             ))