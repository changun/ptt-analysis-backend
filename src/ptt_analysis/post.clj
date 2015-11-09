(ns ptt-analysis.post
  (:require [net.cgrand.enlive-html :as html]))


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
(defn html->post [raw-html]
  (let [body (html/html-snippet raw-html)
        {:keys [author title time ip content]} (parse-author-title-time body)]
    (if (and author title)
      (let [pushes (parse-pushes body)
            length (parse-real-content-length body)]
        {:author author
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
      )
    )
  )
