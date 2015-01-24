(ns ptt-analysis.crawler
  (:import (org.joda.time DateTimeZone DateTime)
           (java.io ByteArrayInputStream)
           (com.amazonaws.services.s3.model AmazonS3Exception)
           (com.luhuiguo.chinese ChineseUtils)
           (java.net URISyntaxException)
           (org.joda.time.format DateTimeFormatter))
  (:require [org.httpkit.client :as http]
            [clojure.core.async :refer [chan <! go put! <!! >! thread] :as async]
            [net.cgrand.enlive-html :as html]
            [clj-time.core :as t]
            [clj-time.format :as f]

            [taoensso.timbre :as timbre
             :refer (log info warn error trace)]
            [cheshire.core :as json]
            [taoensso.timbre.profiling :as profiling
             :refer (pspy pspy* profile defnp p p*)]
            [taoensso.timbre.appenders.carmine :as car-appender]
            [clj-time.coerce :as c]
            [amazonica.aws.s3 :as s3]
            )
  (:use ptt-analysis.async))
; set redis appender
(timbre/set-config! [:appenders :carmine] (car-appender/make-carmine-appender))

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
                              "MenTalk" "feminine_sex" "lesbian" "CATCH" "graduate" "HardwareSale" "Hsinchu" "MayDay" "PublicIssue"]))
(def pool-size 4)
(def ^:dynamic board "Gossiping")

(def options {:timeout 20000             ; ms
              :user-agent "I-am-an-ptt-crawler"
              :basic-auth ["pttrocks" "Cens0123!"]
              :insecure? true
              :headers {"cookie" "over18=1;"}})


(def cred (read-string (slurp "aws.cred")))



(defn async-get-and-retry [& args]
  (go (let [ch (chan 1)]
        (loop  []
          (apply http/get (concat  args [#(if (or (:error %) (= (:status %) 503))
                                           (put! ch (or (:error %) (Exception. (str %))))
                                           (put! ch (:body %)))]))
          (let [ret (<! ch)]
            (cond
              (= (type ret) URISyntaxException)
                (do
                  (warn ret "Wrong URI" args " Message:" ret)
                  nil
                  )
              (error? ret)
                (do
                  (warn ret "Retry:" args " Message:" ret)
                  (Thread/sleep 60000)
                  (recur))
              :else
                ret
              )
            ))
        ))
  )

(defn run-async-with-buffer [fn pool]
  (go-try
    ; park til there is available space in the pool
    (>! pool "")
    (let [ret (<? (fn))]
      ; request finished, release one pool slot
      (<! pool)
      ; return response
      ret
      )
    )
  )
(defn take-all!
  "return a coll containing every results take asyncronously from chs coll.
  Note that the results are not return in order"
  [chs]
  (go-try (let [ch (clojure.core.async/merge chs)]
            (loop [rets []]
              (if-let [ret (<? ch)]
                (recur (conj rets ret))
                rets))
            ))
  )



(defn parse-content [body]
  (let [content-div (:content (into {} (html/select body [:#main-content])))
        article-content (filter (fn [%]
                                  (or (string? %)
                                       (and (= :span (:tag %))
                                            (not= "f2" (get-in % [:attrs :class])))))
                                content-div)
        article-content (map #(if (map? %)
                               (or (clojure.string/join " " (:content %)) "")
                               %
                               ) article-content)
        ]
    (clojure.string/join " " article-content)
    )
  )

(defn parse-real-content-length [body]
  (->> (html/select  body  [:#main-content])
       (first)
       (:content)
       (filter string?)
       (clojure.string/join)
       (#(clojure.string/replace % #"\P{L}" ""))
       (count)
       )
  )
(defn parse-author-title-time [body]
  (let [eles (html/select body [:.article-meta-value])
        [author _ title time] (map html/text eles)
        author  (re-find #"^[^( ]+" author)
        f2s (map html/text (html/select body [:.f2]))
        ips (map #(re-find #"\d+\.\d+\.\d+\.\d+" %) f2s )
        ips (filter seq ips)
        ip (last ips)
        content  (parse-content body)
        ]

       {:author author :title title :time time :ip ip :content content}
    )

  )

(defn parse-pushes [body]
  (let [ops (map html/text (html/select body [:.push :> :.push-tag]))
        user-ids (map html/text (html/select body [:.push :> :.push-userid]))
        ip-dates (map html/text (html/select body [:.push :> :.push-ipdatetime]))
        contents (map html/text (html/select body [:.push :> :.push-content]))
        ]
    (if (= (count ops) (count user-ids) (count ip-dates) (count contents))
      (map (fn [op user-id ip-date content]
             {:op (clojure.string/trim op)
              :user (clojure.string/trim user-id)
              :ip-date (clojure.string/trim ip-date)
              :content (apply str (drop-while #(or (= \space % ) (= \: %)) (or content "")))}
             ) ops user-ids ip-dates contents)
      (throw (Exception. "Push column counts are not consistent!" body))
      )


    )

  )
(defn get-fb-stats [board id]
   (go
     (let [fb-ret (<! (async-get-and-retry (format "http://api.facebook.com/restserver.php?method=links.getStats&urls=https://www.ptt.cc/bbs/%s/%s.html&format=json" board id)))]
       (-> fb-ret
           (json/parse-string true)
           (first)
           )
       )
     )
  )

(defn process-post [post]
  (go-try (try
            (let [raw-body (<? (async-get-and-retry (format "http://www.ptt.rocks/bbs/%s/%s.html" board post) options))
                  {:keys [share_count like_count comment_count click_count] :as fb}
                  (<? (get-fb-stats board post))
                  _ (info fb)
                  body (html/html-snippet raw-body)
                  {:keys [author title time ip content]} (parse-author-title-time body)
                  pushes (parse-pushes body)
                  length (parse-real-content-length body)]

              {:_id post
               :author author
               :title title
               :time time
               :pushed pushes
               :board board
               :ip ip
               :content content
               :length length
               :content-links (map html/text (html/select body [:#main-content :> :a]))
               :push-links  (map html/text (html/select body [:#main-content :.push :a]))
               :fb-share-count share_count
               :fb-like-count like_count
               :fb-comment-count comment_count
               :fb-click-count click_count
               :raw-body raw-body}
              )
            (catch Exception e
              (do (trace e (str board ":" post)) nil)))))

(defn process-page [page-no]

  (go-try (let [get-posts-in-page (fn [page-no]
                      (go-try (let [raw-body (<? (async-get-and-retry (format "http://www.ptt.rocks/bbs/%s/index%d.html" board page-no) options))
                                    body (html/html-snippet raw-body)
                                    posts (for [post-uri (html/select body [:div.title :> :a])]
                                            (let [html (last (clojure.string/split (get-in post-uri [:attrs :href]) #"/" ))]
                                              (subs html 0 (- (count html) 5))))]
                                   posts
                                   )))
                posts (<? (get-posts-in-page page-no))
                pool (chan pool-size)
                chs (loop [[cur & rests] posts chs []]
                      (if cur
                        (recur rests (conj chs (run-async-with-buffer #(process-post cur) pool)))
                        chs
                        )
                      )]
               (filter identity (<? (take-all! chs)))

            )))

(defn str->time [str]
  (try (let [time (.parseDateTime ^DateTimeFormatter (.withZone (f/formatter "E MMM dd HH:mm:ss yyyy") (DateTimeZone/forID "Asia/Taipei"))
                                  (clojure.string/replace str #"\s+" " ")
                                  )]
         (if (t/before? time (t/date-time 2000)) nil time))
       (catch Exception e nil)) )

(defn process-posts-in-page [page board]
  (loop [page page board board]
    (let
      [posts (p :process-page (<!! (binding [board board]
                                     (process-page page))))]

      (if (instance? Exception posts)
        (do (warn posts "Error Retry")
            (Thread/sleep 1000)
            (recur page board))
        (let [posts (map #(assoc % :time (str->time (:time %))) posts)
              posts (filter :time posts)]
          (reverse (sort-by :time posts))
          )
        )
      )
    ))
(defn post-seq
  ([board]
   (let [raw-body (<!! (async-get-and-retry (format "http://www.ptt.rocks/bbs/%s/" board) options))
         body (html/html-snippet raw-body)
         ; get number of pages
         pages  (inc (read-string (second (re-find #"index(\d+)" (get-in (second (html/select body [:a.btn.wide])) [:attrs :href])))))
         ]
     (post-seq board pages)
     ))
  ([board page-no]
   (if (> page-no 0)
     (concat (p :process-one-page (process-posts-in-page page-no board))
             (lazy-seq (post-seq board (dec page-no))))
     [])
   )
  )

(defn upload-to-solr [posts]
  (let [posts (map (fn [{:keys [_id title content author time board pushed length content-links push-links
                                fb-like-count fb-comment-count fb-click-count fb-share-count] :as p}]

                     (let [freqs (frequencies (map :op pushed))
                           pushes (or (get freqs "推") 0)
                           dislike (or (get freqs "噓") 0)
                           arrow (or (get freqs "→") 0)]
                       (try
                         {:id (str board ":" (:_id p))
                          :title title
                          :content (concat [(ChineseUtils/toTraditional
                                              content)]
                                           (map (comp #(ChineseUtils/toTraditional %) :content) pushed)
                                           )
                          :author author
                          :category board
                          :popularity (count pushed)
                          :push pushes
                          :dislike dislike
                          :arrow arrow
                          :fb-like-count fb-like-count
                          :fb-comment-count fb-comment-count
                          :fb-click-count fb-click-count
                          :fb-share-count fb-share-count
                          :length length
                          :board board
                          :content-links content-links
                          :push-links   push-links
                          :isReply (not (nil? (re-matches #"^Re.*" (or title ""))))
                          :last_modified (c/to-string time)}
                         (catch Exception e (warn e p)
                                            (throw e))))
                     ) posts)
        ch (chan 1)
        callback #(if (or (:error %) (not= (:status %) 200))
                   (put! ch (do (error %)
                                (Exception. (str "Query " "Error:" %))))
                   (put! ch (:body %)))]
    (http/post "http://localhost:8983/solr/collection1/update?wt=json&commitWithin=60000"
               {:headers {"Content-Type" "application/json"} :body (json/generate-string posts)} callback)
    (let [ret (<!! ch)]
      (if (error? ret)
        (throw ret)
        ret))
    )

  )
(defn update? [key bytes]
  (try
    (not= (alength bytes) (:instance-length
                            (s3/get-object-metadata cred
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

        (let [ret (s3/put-object cred
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

(defn upload [p]
  (let [
        s3  (upload-to-s3 p)
        solr (upload-to-solr [p])]
    {:s3  s3
     :solr solr})
  )

(defn get-boards []
  (let [hot-boards (map html/text (html/select (html/html-snippet (<!! (async-get-and-retry "https://www.ptt.cc/hotboard.html" options))) [:tr (html/nth-child 2) :a]))]
    (into #{} (concat default-boards hot-boards))
    )
  )
(defn fetch-posts [ago]
  (try
    (profile :info (keyword (str "fetch-post-time" ago))
             (doseq [board-name (get-boards) ]               ;
               (let [min-time  (t/minus (t/now) ago)]
                 (binding [board board-name]
                   (try
                     (loop [[p & posts] (post-seq board) count 0 update 0]
                       (let [{:keys [s3 solr] :as update-ret} (try (upload p) (catch Exception e e))]
                         (cond
                           (error? update-ret)
                             (do (error update-ret p "Upload to solr or s3 failed...")
                                 (Thread/sleep 10000)
                                 (recur (concat [p] posts) count update))
                           (and  (or (t/after?  (:time p) min-time) (< count 20))
                                 (seq posts))
                            (recur posts (inc count) (+ update s3))
                           :else
                            (info (format "[%s] %s Done %s Posts %s Updated, Last Post %s" ago board count update (:time p)))
                           )
                         )
                       )
                     (catch Exception e (error e))))
                 )))
    (catch Exception e (error e))
    )
  )

(defn periodic-fetch-posts [ago delay]
  (loop []
    (fetch-posts ago)
    (Thread/sleep delay)
    (recur)
    )
  )

(defn start []
  (thread (periodic-fetch-posts (t/days 30)  60000))
  (thread (periodic-fetch-posts (t/hours 24)  1000))
  (thread (periodic-fetch-posts (t/hours 2)   1000)))


