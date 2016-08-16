(ns ptt-analysis.post
  (:require [net.cgrand.enlive-html :as html]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [clj-time.coerce :as c]

            [schema.core :as s])
  (:use [ptt-analysis.schema])
  (:import
    (org.joda.time DateTimeZone DateTime)

    (java.util Arrays)))


(defn parse-content [body]
  (let [content-div (:content (into {} (html/select body [:#main-content])))
        article-content (filter (fn [%]
                                  (or (string? %)
                                      (and (= :span (:tag %))
                                           (not= "f2" (get-in % [:attrs :class])))))
                                content-div)
        article-content (html/texts article-content)
        ]
    (clojure.string/join " " article-content)
    )
  )




(defn content-and-ip [body]
  (let [f2s (map html/text (html/select body [:.f2]))
        ips (map #(re-find #"\d+\.\d+\.\d+\.\d+" %) f2s )
        ips (filter seq ips)
        ip (last ips)
        content  (parse-content body)]
    {:ip ip :content content})

  )
(defn post-hash [post]
  (Arrays/hashCode (.getBytes post "UTF-8")))

(def date-time-format (.withZone (f/formatter "M/dd HH:mm") (DateTimeZone/forID "Asia/Taipei")))

(s/defn parse-pushes :- [Push]
  [body time]
  (let [pushes (html/select body [:.push])
        pushes (map (fn [push]
                      (let [by-class  #(clojure.string/trim (html/text (first (html/select push [%]))))
                            op (by-class :.push-tag)
                            user-id (by-class :.push-userid)
                            ip-date (by-class :.push-ipdatetime)
                            content  (by-class :.push-content)
                            time (try
                                   (let [date-time (f/parse date-time-format ip-date)
                                         year (min-key
                                                #(Math/abs
                                                  (- (c/to-long (.withYear ^DateTime date-time %))
                                                     (c/to-long time)))
                                                (t/year time) (inc (t/year time)) (dec (t/year time)))]
                                     (.withYear date-time year)
                                     )
                                   (catch Exception _
                                     ))

                            ]
                        (if (and (seq op) (seq user-id) )
                          {:op      op
                           :user    user-id
                           :time    time
                           :content (apply str (drop-while #(or (= \space %) (= \: %)) (or content "")))})
                        )

                      ) pushes)]
    (filter identity pushes)
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

(defn id->time [id]
  (-> (re-matches #"M.(\d+)\.\w\.[\d\w]+" id)
      (second )
      (Long/valueOf )
      (* 1000 )
      (c/from-long)
      (.withZone  (DateTimeZone/forID "Asia/Taipei"))
      )
  )
(defn extract-title [body]
  (->
      body
      (html/select  [(html/attr= :property "og:title") ])
      (first)
      (get-in [:attrs :content])
      )
  )
(s/defn html->post :- Post [{:keys [id raw-body title] :as ret}]
  (let [body (html/html-snippet raw-body)
        {:keys [ip content]}
        (content-and-ip body)
        title
        (or title (extract-title body))
        time (id->time id)
        ]
    (let [pushes (parse-pushes body time)
          length (parse-real-content-length body)]
      (merge
        ret
        {:time time
         :pushed pushes
         :content content
         :ip ip
         :title title
         :length length
         :content-links (map html/text (html/select body [:#main-content :> :a]))
         :push-links  (map html/text (html/select body [:#main-content :.push :a]))
         }
        )
      )
    )
  )
