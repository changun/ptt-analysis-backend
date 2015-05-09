(ns ptt-analysis.crawler
  (:import (org.joda.time DateTimeZone)
           (java.io ByteArrayInputStream InputStream)
           (com.amazonaws.services.s3.model AmazonS3Exception)
           (com.luhuiguo.chinese ChineseUtils)
           (org.joda.time.format DateTimeFormatter)
           (com.fasterxml.jackson.databind.util ByteBufferBackedInputStream)

           (java.nio ByteBuffer))
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
            [amazonica.aws.lambda :as lambda]
            [clojure.edn]
            )
  (:use ptt-analysis.async))



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



(def get-cred (memoize (fn [] (clojure.edn/read-string (slurp "aws.cred")))) )


(defn lambda-invoke [payload]
  (lambda/invoke (get-cred)
                 :function-name "httpRequest"
                 :payload payload
  ))
(defn ptt-endpoint-lambda
  "Make http request to ptt.cc via AWS Lambda"
  [path]
  (let [payload (-> {:host "www.ptt.cc"
                     :path path
                     :headers {"cookie" "over18=1;"}}
                    (json/generate-string)

                    )
        ; there is a bug in Lambda wrapper in determinig the payload type
        ; here we walkaround the bug by try usign String first,
        ; if it does not work, then use nio ByteBuffer
        {:keys [status-code payload]} (try (lambda-invoke payload)
                                           (catch Exception _
                                             (lambda-invoke (-> payload
                                                                (.getBytes)
                                                                (ByteBuffer/wrap)))))]
    (if (= status-code 200)
      (-> payload
          (ByteBufferBackedInputStream.)
          (clojure.java.io/reader)
          (json/parse-stream true)
          )
      (do (error status-code payload)
          {:error (str status-code payload)})
      )
    )
  )
(defn ptt-endpoint-local
  "Make http request to ptt.cc via distributed proxies"
  [path]
  @(http/get (str "http://localhost/proxies/proxy?url=https://www.ptt.cc" path)
             {:headers {"cookie" "over18=1;"}
              :basic-auth ["pttrocks" "Cens0123!"]
              :timeout 10000             ; ms
              :user-agent "I-am-an-ptt-crawler"
              :insecure? true})
  )
(def ptt-endpoint
  "The main endpoint to ptt. It optimize the performance by using mutiple way to access ptt pages,
  including AWS Lambda function and distributed proxy servers (see http://github.com/changun/proxy).
  It handlers auto-try with expotential backoff and periodically print performance metrics.
  "
  ; performance metrics
  (let [speed-meter (atom 0)
        total-count (atom 0)
        error-count (atom 0)]
    (fn [path]
      (let [start (t/now)]
        (loop [fail 0 force-local false]
          ; favor lamdba endpoint becuase it is cheaper?
          ; when force-local, always use local endpoint as some large page is only accessible via local endpoint
          (let [{:keys [status body] :as ret} (try (if (and (> (rand) 0.1 ) (not force-local))
                                                (ptt-endpoint-lambda path)
                                                (ptt-endpoint-local path))
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
  (if-let [headers (seq (html/select body [:.article-meta-value]))]
    (let [[author _ title time] (map html/text headers)
          author  (re-find #"^[^( ]+" author)
          f2s (map html/text (html/select body [:.f2]))
          ips (map #(re-find #"\d+\.\d+\.\d+\.\d+" %) f2s )
          ips (filter seq ips)
          ip (last ips)
          content  (parse-content body)]
      {:author author :title title :time time :ip ip :content content})
    )

  )

(defn parse-pushes [body]
  (let [pushes (html/select body [:.push])
        pushes (map (fn [push]
                      (let [op (html/text (first (html/select push [:.push-tag])))
                            user-id (html/text (first (html/select push [:.push-userid])))
                            ip-date (html/text (first (html/select push [:.push-ipdatetime])))
                            content  (html/text (first (html/select push [:.push-content])))
                            ]
                        (if (and (seq op) (seq user-id) )
                          {:op (clojure.string/trim op)
                           :user (clojure.string/trim user-id)
                           :ip-date (clojure.string/trim ip-date)
                           :content (apply str (drop-while #(or (= \space % ) (= \: %)) (or content "")))})
                        )

                      ) pushes)]
    (filter identity pushes)


    )

  )

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

(defn process-post [board post]
  (let [{:keys [status body]} (ptt-endpoint (format "/bbs/%s/%s.html" board post))]
    (if (= 200 status)
      (let [raw-body body
            body (html/html-snippet body)
            {:keys [author title time ip content]} (parse-author-title-time body)]
        (if (and author title)
          (let [{:keys [share_count like_count comment_count click_count] :as fb}
                (get-fb-stats board post)
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
          (trace "Skip" board post title " which has no auhtor")
          )
        ))
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
                     ) posts)]

    (let [{:keys [body status]} @(http/post "http://localhost:8983/solr/collection1/update?wt=json&commitWithin=60000"
                                           {:headers {"Content-Type" "application/json"}
                                            :body (json/generate-string posts)})]
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
        (info (format "[%s] %s Done %s Posts %s Updated, Last Post %s" ago board count update (:time (last all-posts))))
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
        ]
    (into #{} (concat default-boards hot-boards))
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
  )

(defn start []
  (thread (periodic-fetch-posts (t/days 30)  60000))
  (thread (periodic-fetch-posts (t/hours 24)  1000))
  (thread (periodic-fetch-posts (t/hours 2)  1000))
  )


