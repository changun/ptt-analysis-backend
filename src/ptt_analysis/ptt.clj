(ns ptt-analysis.ptt
  (:require [net.cgrand.enlive-html :as html]
            [taoensso.timbre
             :refer (log info warn error trace)]
            [schema.core :as s]
            [ptt-analysis.solr :as solr]
            [ptt-analysis.post :as post]
            [ptt-analysis.facebook :as fb])

  (:use [ptt-analysis.endpoint]
        [ptt-analysis.schema])
  )

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

(defn hot-boards []
  (->
    (ptt-endpoint "/hotboard.html")
    (:body)
      (html/html-snippet)
      (html/select  [:tr (html/nth-child 2) :a])
      (->> (map html/text )
           (map #(first (clojure.string/split % #"\|")))
           )



      )
  )



(defn get-raw-post [{:keys [board id string-hash]} ]
  (ptt-endpoint (format "/bbs/%s/%s.html" board id)
                {:headers {"string-hash" (str string-hash)}}
                )
  )




(defn post-ids
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
     (post-ids board pages)
     ))
  ([board page-no]
   (if (> page-no 0)
     (let [{:keys [status body]} (ptt-endpoint (format "/bbs/%s/index%d.html" board page-no))
           raw-body body
           body (html/html-snippet raw-body)
           posts (for [post-entry (->> (html/select body [:div.bbs-screen :> :div])
                                     ; take until we hit the ret-sep
                                     (take-while #(= (get-in % [:attrs :class]) "r-ent") )

                                     )]
                   (do
                     (if-let  [url-entry (first (html/select post-entry [:div.title :> :a]))]
                       (let [url
                             (->
                               url-entry
                               (get-in  [:attrs :href])
                               (clojure.string/split  #"/" )
                               (last)
                               )
                             title
                             (-> (html/select post-entry [:div.title :> :a])
                                 (first)
                                 (html/text)
                                 )
                             author
                             (-> (html/select post-entry [:div.meta :> :div.author])
                                 (first)
                                 (html/text  )
                                 )
                             ]
                         {:id (subs url 0 (- (count url) 5))
                          :title (if (< (count title) 50)
                                   title)
                          :author author
                          :board board})

                       )
                     )


                   )
           posts (->> (reverse posts)
                      (filter identity)
                      )
           ]
       (concat posts (lazy-seq (post-ids board (dec page-no))))
       )
     )
    )
  )