(ns ptt-analysis.more-like-this
  (:import (org.joda.time DateTimeZone DateTime)
           (com.luhuiguo.chinese ChineseUtils))
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
        ptt-analysis.async)
  )

(defn get-more-like-this [news popularity]
  (go-try (let [body (<? (async-post  "http://localhost:8983/solr/mlt"
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
                                               (apply str (:content news) (repeat 5 (:title news))))}))
                body (json/parse-string body true)]
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
  (if (seq coll)
    (if (> (count coll) 1)
      (let [avg (/ (apply + coll) (count coll))
            sd-square (/ (apply + (map #(Math/pow (double (- avg %)) 2) coll)) (count coll))]
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
(defn order-weight [{:keys [n-pop n-push n-score n-recent n-fb-total]}]
  (+ (* n-push 5)(* n-pop 3) (* n-score 5) (* n-recent 10) (* n-fb-total 7))
  )
(defn execellent-weight [{:keys [n-pop n-push n-score n-recent n-fb-total]}]
  (+ (* n-push 10) (* n-score 5) (* n-recent 10) (* n-fb-total 7))
  )
(defn best-match-weight [{:keys [n-pop n-score n-recent]}]
  (+ (* n-pop 2) (* n-score 10) (* n-recent 8))
  )

(defn push-rate [{:keys [push last_modified]}]
  (/ (or push 0) (t/in-minutes (t/interval (c/from-string last_modified) (t/now))))
  )

(defn image-urls [urls]

  )

(defn excellent-article? [{:keys [push dislike length title push-rate fb-total content-links]}]
  (and
    (not (re-matches #"^(Fw:)?\s*\[?新聞\]?.*" (or title "")))
    (not (re-matches #"^(Fw:)?\s*\[?問卦\]?.*" (or title "")))
    (not (re-matches #"^(Fw:)?\s*\[?公告\]?.*" (or title "")))
    (and
      (or (> push 60)
          (and (> push 20) (> push-rate 0.2))
          (> fb-total 50)
          )
      (> (/ push (+ dislike push)) 0.7)
      (or (> length 150)
          (seq content-links))
      ))
  )



(defn search-handler [req]
  (with-channel req channel
                (let [start-time (System/nanoTime)
                      {:keys [title content href] :as news} (try (cheshire.core/parse-string (slurp (:body req)) true) (catch Exception _ nil))]
                  (if (and title content)
                    (go (try
                          (let [matches (filter #(> (:score %) 0.15) (<? (get-more-like-this news 1)))
                                ; make sure there is no nil
                                matches (map (fn [{:keys [push dislike arrow popularity length fb-share-count fb-comment-count fb-like-count] :as m}]
                                               (assoc m :push (or push 0)
                                                        :dislike (or dislike 0)
                                                        :arrow (or arrow 0)
                                                        :popularity (or popularity 0)
                                                        :length (or length 151)
                                                        :fb-total (apply + (map #(or % 0) [fb-share-count fb-comment-count fb-like-count])))
                                               ) matches)
                                ; set a cap on popularity
                                matches (map (fn [{:keys [push dislike arrow popularity] :as m}]
                                               (assoc m
                                                      :push (if (> push 150) 150 push)
                                                      :dislike (if (> dislike 150) 150 dislike)
                                                      :arrow (if (> arrow 150) 150 arrow)
                                                      :popularity (if (> popularity 200) 200 popularity))
                                               ) matches)
                                matches (map (fn [m] (assoc m :push-rate (push-rate m)) ) matches)
                                matches (map (fn [pop push score fb-total recent match]
                                               (assoc match
                                                      :n-pop pop
                                                      :n-push push
                                                      :n-score score
                                                      :n-fb-total fb-total
                                                      :n-recent recent
                                                      )
                                               )
                                             (normalize (map :popularity matches))
                                             (normalize (map :push matches))
                                             (normalize (map :score matches))
                                             (normalize (map :fb-total matches))
                                             (normalize (map (comp
                                                               c/to-epoch
                                                               #(.withTimeAtStartOfDay ^DateTime %)
                                                               #(t/to-time-zone %(DateTimeZone/forID "Asia/Taipei"))
                                                               c/from-string
                                                               :last_modified
                                                               ) matches))
                                             matches)

                                matches (reverse (sort-by order-weight matches))

                                best-match (first (reverse (sort-by best-match-weight
                                                                    (filter (fn [{:keys [title]}]
                                                                              (and (not= title "")
                                                                                   (not= title nil)
                                                                                   (not (re-matches #"^Re:.*" title))) )
                                                                            matches))))

                                excellent-articles (reverse (sort-by :last_modified (take 6 (reverse (sort-by execellent-weight  (filter excellent-article? matches))))))
                                excellent-article-ids (into #{} (map :id excellent-articles))
                                response (-> {:articles
                                              (->> matches
                                                   (filter #(not (contains? excellent-article-ids (:id %))))
                                                   (take 10)
                                                   (sort-by :last_modified)
                                                   (reverse))
                                              }
                                             (assoc :best-match best-match)
                                             (assoc :excellent-articles excellent-articles)
                                             )

                                ]
                            (send! channel (merge default-response
                                                  {:body (cheshire.core/generate-string response)}))
                            (info "Served request:" req "in" (/ (- (System/nanoTime) start-time) 1e6))
                            )
                          (catch Exception e (do (error e "search error!" req)
                                                 (send! channel {:status 400
                                                                 :body (cheshire.core/generate-string
                                                                         {:reason "Something went wrong..."}) })

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


