(ns ptt-analysis.facebook
  (:require [cheshire.core :as json]
            [schema.core :as s]
            [org.httpkit.client :as http])
  (:use [ptt-analysis.schema])
  )

(def fb-stats-format "http://api.facebook.com/restserver.php?method=links.getStats&urls=https://www.ptt.cc/bbs/%s/%s.html&format=json")

(s/defn ^:always-validate stats :- FaceStats [board id]
  (let [{:keys [body status]}
        @(http/get
           (format fb-stats-format board id))]

    (if (= 200 status)
      (let [{:keys [share_count like_count comment_count click_count]}
            (-> body
                (json/parse-string true)
                (first)
                )]
        {:fb-share-count share_count
         :fb-like-count like_count
         :fb-comment-count comment_count
         :fb-click-count click_count}
        )
      (recur board id))
    )
  )