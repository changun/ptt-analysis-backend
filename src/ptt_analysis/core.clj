(ns ptt-analysis.core
  (:import (org.joda.time DateTimeZone))
  (:require [org.httpkit.client :as http]
            [clojure.core.async :refer [chan <! go put! <!! >! merge]]
            [net.cgrand.enlive-html :as html]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [monger.core :as mg]
            [monger.collection :as mc]))

(def pool-size 3)
(def ^:dynamic board "Gossiping")
(def options {:timeout 20000             ; ms
              :user-agent "I-am-an-ptt-crawler"
              :insecure? true
              :headers {"cookie" "over18=1;"}})
(defn error? [%] (instance? Exception %))

(defmacro if-not-error [binding & clauses]
  `(let [~(first binding) ~(second binding)
         ]
     (if (error? ~(first binding)) ~(first binding)
                                   (do ~@clauses))
     ))
(defmacro go-try [& clauses]
  (let [s (gensym)]
    `(go (try ~@clauses (catch Exception ~s ~s)))
    ))

(defmacro <? [clause]
  (let [s (gensym)]
    `(let [~s (<! ~clause) ]
       (if (error? ~s)
         (throw ~s)
         ~s)
       )))


(defn async-get [& args] (let [ch (chan 1)]
                           (apply http/get (concat  args [#(if (or (:error %) (not= (:status %) 200))
                                                            (put! ch (Exception. (str "Query " args "Error:" %)))
                                                            (put! ch (html/html-snippet (:body %))))]))
                           ch
                           ))
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
  (go-try (let [ch (merge chs)]
            (loop [rets []]
              (if-let [ret (<? ch)]
                (recur (conj rets ret))
                rets))
            ))
  )

(defn parse-pages [body]
  (read-string (second (re-find #"index(\d+)" (get-in (second (html/select body [:a.btn.wide])) [:attrs :href]))))
  )
(defn parse-posts [body]
  (let [posts (html/select body [:div.title :> :a])]
    (map #(let [html (last (clojure.string/split (get-in % [:attrs :href]) #"/" ))
                post (subs html 0 (- (count html) 5))]
           post) posts))

  )
(defn parse-author-title-time [body]
  (let [eles (html/select body [:.article-meta-value])
        [author _ title time] (map html/text eles)
        author  (re-find #"^[^( ]+" author)
        f2s (map html/text (html/select body [:.f2]))
        ips (map #(re-find #"\d+\.\d+\.\d+\.\d+" %) f2s )
        ips (filter seq ips)
        ip (last ips)
        ]

       {:author author :title title :time time :ip ip}
    )

  )
(defn parse-pushes [body]
  (let [ops (map html/text (html/select body [:.push :> :.push-tag]))
        user-ids (map html/text (html/select body [:.push :> :.push-userid]))
        ip-dates (map html/text (html/select body [:.push :> :.push-ipdatetime]))
        ]
        (map (fn [op user-id ip-date]
               {:op (clojure.string/trim op)
                :user (clojure.string/trim user-id)
                :ip-date (clojure.string/trim ip-date)}
               ) ops user-ids ip-dates)
    )

  )

(defn get-posts-in-page [page-no]
  (go-try (let [body (<? (async-get (format "https://www.ptt.cc/bbs/%s/index%d.html" board page-no) options))
                posts (parse-posts body)]
            posts
            )))

(defn get-post [post]
  (go-try (try
            (let [body (<? (async-get (format "https://www.ptt.cc/bbs/%s/%s.html" board post) options))
                  {:keys [author title time ip]} (parse-author-title-time body)
                  pushes (parse-pushes body)]

              {:_id post :author author :title title :time time :pushed pushes :board board :ip ip}
              )
            (catch Exception _ nil))))

(defn process-page [page-no]
  (go-try (let [posts (<? (get-posts-in-page page-no))
                pool (chan pool-size)
                chs (loop [[cur & rests] posts chs []]
                      (if (seq rests)
                        (recur rests (conj chs (run-async-with-buffer #(get-post cur) pool)))
                        chs
                        )
                      )]
               (filter identity (<? (take-all! chs)))

            )))

(defn str->time [str]
  (try (let [time (.parseDateTime (.withZone (f/formatter "E MMM dd HH:mm:ss yyyy") (DateTimeZone/forID "Asia/Taipei"))
                                  (clojure.string/replace str #"\s+" " ")
                                  )]
         (if (t/before? time (t/date-time 2000)) nil time))
       (catch Exception e nil)) )


(loop [run 1]
  (let [conn (mg/connect)
        db   (mg/get-db conn "ptt-analysis")]
    (doseq [board-name [ "Gossiping" "sex" "LoL" "NBA" "Baseball"
                         "WomenTalk" "C_Chat" "ToS" "Boy-Girl" "movie"
                         "MobileComm" "PathofExile" "iPhone" "joke"
                         "PuzzleDragon" "marvel" "MLB" "Beauty"
                         "KR_Entertain" "e-shopping" "StupidClown" "AllTogether"
                         "Food" "HatePolitics" "Wanted" "gay" "Tech_Job"]]
      (binding [board board-name]
        (let [body (<!! (async-get (format "https://www.ptt.cc/bbs/%s/" board) options))
              pages  (parse-pages body)
              ]
          (loop [page pages total 0]
            (print "Board: " board " Page:" page)
            (let [one-week-ago (t/minus (t/now) (t/weeks 1))
                  posts (<!! (process-page page))]
              (if (instance? Exception posts)
                (do (println "Error Retry")
                    (Thread/sleep 10000)
                    (recur page total))
                (let [posts (filter :time posts)
                      times (map #(str->time (:time %)) posts)
                      times (filter identity times)]

                  (doseq [post posts]
                    (mc/save db "posts" post))
                  (print (format " Inserted %d posts (%d)\n"
                                 (count posts)
                                 (+ total (count posts)) ) )
                  (let [min-time (if (seq times) (reduce #(if (t/before? %1 %2) %1 %2) times) nil)]
                    (print " Time" min-time)
                    (if (or (nil? min-time) (t/after? min-time one-week-ago))
                      (recur (- page 1) (+ total (count posts)))
                      total
                      ))
                  )
                )
              ))))
      )
    )
  (Thread/sleep (* 1000 60 60 ))
  (print "Finish Run " run)
  (recur (+ 1 run))
  )

