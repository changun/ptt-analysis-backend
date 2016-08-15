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
    @(ptt-endpoint "/hotboard.html")
    (:body)
      (html/html-snippet)
      (html/select  [:tr (html/nth-child 2) :a])
      (->> (map html/text ))
      )
  )



(defn post-raw [board post-id string-hash & [callback]]
  (ptt-endpoint (format "/bbs/%s/%s.html" board post-id)
                {:headers {"string-hash" (str string-hash)}}
                callback)
  )




(defn post-ids
  ([board]
   (let [{:keys [status body] :as ret} @(ptt-endpoint (format "/bbs/%s/index.html" board))
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
     (let [{:keys [status body]} @(ptt-endpoint (format "/bbs/%s/index%d.html" board page-no))
           raw-body body
           body (html/html-snippet raw-body)

           posts (for [post-uri (->> (html/select body [:div.bbs-screen :> :div])
                                     ; take until we hit the ret-sep
                                     (take-while #(= (get-in % [:attrs :class]) "r-ent") )
                                     (map #(first (html/select % [:div.title :> :a])))
                                     (filter identity)
                                     )]

                   (let [html (last (clojure.string/split (get-in post-uri [:attrs :href]) #"/" ))]
                     (subs html 0 (- (count html) 5))))
           posts (reverse posts)
           ]
       (concat posts (lazy-seq (post-ids board (dec page-no))))
       )
     )
    )
  )