(ns ptt-analysis.post
  (:require [net.cgrand.enlive-html :as html]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [clj-time.coerce :as c]

            [schema.core :as s])
  (:use [ptt-analysis.schema])
  (:import
    (org.joda.time DateTimeZone DateTime)
    (org.joda.time.format DateTimeFormatter)
    ))


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

(def time-formatter (.withZone (f/formatter "E MMM dd HH:mm:ss yyyy") (DateTimeZone/forID "Asia/Taipei")))

(defn str->time [str]
  (let [time (->> (clojure.string/replace str #"\s+" " ")
                  (.parseDateTime ^DateTimeFormatter time-formatter))]
    (if (< (t/year time) 1990)
      (throw (RuntimeException. (str "Time string error" str)))
      time
      )
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
      {:author author :title title :time (str->time time) :ip ip :content content})
    )

  )
(defn post-hash [post]
  (java.util.Arrays/hashCode (.getBytes post "UTF-8")))

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

(s/defn html->post :- Post [board id raw-html]
  (let [body (html/html-snippet raw-html)
        {:keys [author title ip content time]}
        (parse-author-title-time body)
        time (or time
                 (-> (re-matches #"M.(\d+)\.\w\.[\d\w]+" id)
                     (second )
                     (Long/valueOf )
                     (* 1000 )
                     (c/from-long)
                     (.withZone  (DateTimeZone/forID "Asia/Taipei"))
                     ))
        ]
    (if (and author title)
      (let [pushes (parse-pushes body time)
            length (parse-real-content-length body)]
        {:id id
         :board board
         :raw-body raw-html
         :author author
         :title title
         :time time
         :pushed pushes
         :ip ip
         :content content
         :length length
         :content-links (map html/text (html/select body [:#main-content :> :a]))
         :push-links  (map html/text (html/select body [:#main-content :.push :a]))
         }
        )
      (throw (RuntimeException. (str board ":" id " author or title is missing:" author title) ))
      )
    )
  )
