(ns ptt-analysis.crawler
  (:import (org.joda.time DateTimeZone)
           (java.io ByteArrayInputStream)
           (com.amazonaws.services.s3.model AmazonS3Exception)
           (com.luhuiguo.chinese ChineseUtils)
           (org.joda.time.format DateTimeFormatter)


           (java.util Arrays))
  (:require [org.httpkit.client :as http]
            [clojure.core.async :refer [chan <! go put! <!! >! thread]]
            [net.cgrand.enlive-html :as html]
            [clj-time.core :as t]
            [clj-time.format :as f]

            [taoensso.timbre :as timbre
             :refer (log info warn error trace)]
            [cheshire.core :as json]
            [taoensso.timbre.profiling :refer (pspy pspy* profile defnp p p*)]
            [clj-time.coerce :as c]
            [amazonica.aws.s3 :as s3]
            [ptt-analysis.solr :as solr]
            [ptt-analysis.post :as post]
            [clojure.edn]
            )
  (:use [ptt-analysis.async]
        [ptt-analysis.endpoint]))



(DateTimeZone/setDefault (DateTimeZone/forID "Asia/Taipei"))
(def default-boards (into #{} ["Gossiping" "NBA" "sex" "LoL" "Stock" "Baseball"
                              "WomenTalk" "HatePolitics" "WOW" "ToS" "BuyTogether"
                              "C_Chat" "e-shopping" "movie" "joke" "marvel" "Japan_Travel"
                              "Boy-Girl" "MobileComm" "BabyMother" "Food" "PuzzleDragon"
                              "NBA_Film" "StupidClown" "Tech_Job" "Beauty" "KR_Entertain"
                              "iPhone" "car" "Lifeismoney" "ChainChron" "Hearthstone" "Tainan" "AllTogether"
                              "marriage" "home-sale" "Lakers" "mobilesales" "part-time" "Jeremy_Lin"
                              "Kaohsiung" "Aviation" "MakeUp" "ONE_PIECE" "Wanted" "TaichungBun" "SuperJunior"
                              "creditcard" "Japandrama" "HelpBuy" "gay" "GetMarry" "Examination" "PathofExile"
                              "japanavgirls" "BeautySalon" "Salary" "PC_Shopping" "Option" "SportLottery"
                              "MenTalk" "feminine_sex" "lesbian" "CATCH" "graduate" "HardwareSale" "Hsinchu"
                               "MayDay" "PublicIssue" "cat" "dog" ]))



(def get-cred (memoize (fn [] (clojure.edn/read-string (slurp "aws.cred")))) )



(defn get-fb-stats [board id]
  (let [{:keys [body status]} @(http/get (format "http://api.facebook.com/restserver.php?method=links.getStats&urls=https://www.ptt.cc/bbs/%s/%s.html&format=json" board id)) ]
    (if (= 200 status)
      (-> body
          (json/parse-string true)
          (first)
          )
      (recur board id))
    )
  )

(defn process-post [board post-id]
  (let [string-hash (:string-hash (solr/get-post board post-id))
        {:keys [status body]} (ptt-endpoint (format "/bbs/%s/%s.html" board post-id) string-hash)]
    (cond
      (= 200 status)
      (if-let [post (post/html->post body)]
        (let [{:keys [share_count like_count comment_count click_count] :as fb}
              (get-fb-stats board post-id)]
          (merge post
                 {:_id post-id
                  :fb-share-count share_count
                  :fb-like-count like_count
                  :fb-comment-count comment_count
                  :fb-click-count click_count
                  :raw-body body
                  }
                 )
          )
        (trace "Skip" board post-id " which has no auhtor")
        )
      (= 201 status)
      (trace "Skip" board post-id "due to the same string-hash")

      )
    )
  )




(defn str->time [str]
  (try (let [time (.parseDateTime ^DateTimeFormatter (.withZone (f/formatter "E MMM dd HH:mm:ss yyyy") (DateTimeZone/forID "Asia/Taipei"))
                                  (clojure.string/replace str #"\s+" " ")
                                  )]
         (if (t/before? time (t/date-time 2000)) nil time))
       (catch Exception e nil)) )


(defn post-id-sequence
  ([board]
   (let [{:keys [status body] :as ret} (ptt-endpoint (format "/bbs/%s/index.html" board))
         raw-body body
         body (html/html-snippet raw-body)
         ; get number of pages
         pages  (try
                  (-> (html/select body [:a.btn.wide])
                      (second)
                      (get-in  [:attrs :href])
                      (->> (re-find #"index(\d+)"))
                      (second)
                      (read-string)
                      (inc)
                      )
                  (catch Exception e (error e "Can't parse number of pages" board ret))
                  )
         ]
     (post-id-sequence board pages)
     ))
  ([board page-no]
   (if (> page-no 0)
     (let [{:keys [status body]} (ptt-endpoint (format "/bbs/%s/index%d.html" board page-no))
           raw-body body
           body (html/html-snippet raw-body)

           posts (for [post-uri (->> (html/select body [:div.bbs-screen :> :div])
                                     ; take until we hit the ret-sep
                                     (take-while #(= (get-in % [:attrs :class]) "r-ent") )
                                     (map #(first (html/select % [:div.title :> :a] )) )
                                     (filter identity)
                                     )]

                   (let [html (last (clojure.string/split (get-in post-uri [:attrs :href]) #"/" ))]
                     (subs html 0 (- (count html) 5))))
           posts (reverse posts)
           ]
       (concat posts (lazy-seq (post-id-sequence board (dec page-no))))
       )
     )
    )
  )

(defn post-seq [board]
  (->> (post-id-sequence board)
       (map (fn [p] (future (process-post board p))))
       (partition 20)
       (apply concat)
       (map #(identity @%))
       (filter identity)
       (map #(assoc % :time (str->time (:time %))))
       (filter :time))
  )

(defn upload-to-solr [posts]
  (let [posts (map (fn [{:keys [_id title content author time board pushed length content-links push-links
                                fb-like-count fb-comment-count fb-click-count fb-share-count raw-body] :as p}]

                     (let [freqs (frequencies (map :op pushed))
                           pushes (or (get freqs "推") 0)
                           dislike (or (get freqs "噓") 0)
                           arrow (or (get freqs "→") 0)]
                       (try
                         {:id               (str board ":" (:_id p))
                          :title            title
                          :content          (concat [(ChineseUtils/toTraditional
                                                       content)]
                                                    (map (comp #(ChineseUtils/toTraditional %) :content) pushed)
                                                    )
                          :author           author
                          :category         board
                          :popularity       (count pushed)
                          :push             pushes
                          :dislike          dislike
                          :arrow            arrow
                          :fb-like-count    fb-like-count
                          :fb-comment-count fb-comment-count
                          :fb-click-count   fb-click-count
                          :fb-share-count   fb-share-count
                          :length           length
                          :board            board
                          :content-links    content-links
                          :push-links       push-links
                          :isReply          (not (nil? (re-matches #"^Re.*" (or title ""))))
                          :last_modified    (c/to-string time)
                          :string-hash      (Arrays/hashCode (.getBytes raw-body "UTF-8"))
                          }
                         (catch Exception e (warn e p)
                                            (throw e))))
                     ) posts)]

    (let [{:keys [body status]} (solr/add-posts posts)]
      (if-not (= 200 status)
        (throw (Exception. (str status body)))
        body))
    )
  )
(defn update? [key bytes]
  (try
    (not= (alength bytes) (:instance-length
                            (s3/get-object-metadata (get-cred)
                                                    :bucket-name "ptt.rocks"
                                                    :key key)))
       (catch AmazonS3Exception e
              (do (trace "key" key "does not exist")
                  true
                  ))
       )
  )
(defn upload-to-s3 [{:keys [_id board raw-body] :as p}]
  (let [bytes (.getBytes raw-body "UTF-8")
        input (ByteArrayInputStream. bytes)
        key (str "posts/" board "/" _id ".html")
        length (alength bytes)]

    (if (update? key bytes)
      (do

        (let [ret (s3/put-object (get-cred)
                                 :bucket-name "ptt.rocks"
                                 :key key
                                 :input-stream input
                                 :metadata {:content-length length}
                                 :return-values "ALL_OLD")]
          (trace key ret))
          1)
      (do (trace "Key:" key "did not change.")
          0)
      )
    )
  )



(defn fetch-posts [board ago]
  (let [min-time  (t/minus (t/now) ago)
        all-posts (take-while #(t/after?  (:time %) min-time) (post-seq board))]
    (loop [[p & ps :as posts] all-posts count 0 update 0]
      (if p
        (let [ret (try
                    (upload-to-solr [p])
                    (upload-to-s3 p)
                    (catch Exception e e))]
          (if (error? ret)
            (do
              (error ret p "Upload to solr or s3 failed...")
              (Thread/sleep 10000)
              (recur posts count update))
            (recur ps (inc count) (+ update ret))
            )

          )
        (info (format "[%s] %s Done %s Posts %s Updated" ago board count update))
        )
      )

    )
  )


(defn get-boards []
  (let [{:keys [body status] } (ptt-endpoint "/hotboard.html")
        hot-boards (-> body
                       (html/html-snippet)
                       (html/select  [:tr (html/nth-child 2) :a])
                       (->> (map html/text ))
                       )
        ;http://www.ptt.rocks/solr/collection1/select?q=*%3A*&rows=0&wt=json&indent=true&facet=true&facet.field=board
        existing-boards (try
                          (solr/get-all-boards)
                          (catch Exception _ nil))
        ]
    (into #{} (concat default-boards hot-boards existing-boards))
    )
  )

(defn periodic-fetch-posts [ago delay]
  (loop []
    (try
      (profile :info (keyword (str "fetch-post-time" ago))
               (doseq [board (get-boards)]
                 (try
                   (fetch-posts board ago)
                   (catch Exception e (error e)))
                 ))
      (catch Exception e (error e))
      )
    (Thread/sleep delay)
    (recur)
    )
  (solr/optimize)
  )

(defn start []
  (thread (periodic-fetch-posts (t/days 7)  3600000))
  (thread (periodic-fetch-posts (t/hours 24)  1000))
  )


