(ns ptt-analysis.facebook
  (:require [cheshire.core :as json]
            [schema.core :as s]
            [org.httpkit.client :as http])
  (:use [ptt-analysis.schema])
  )

(def fb-stats-format "http://graph.facebook.com/?id=https://www.ptt.cc/bbs/%s/%s.html")


; As of August 2016. We can only get comment count and share count
(s/defn ^:always-validate stats :- FaceStats [board id]
  (let [{:keys [body status]}
        @(http/get
           (format fb-stats-format board id))]

    (if (= 200 status)
      (let [{:keys [share_count like_count comment_count click_count]}
            (-> body
                (json/parse-string true)
                (:share)
                )]
        {:fb-share-count share_count
         :fb-like-count (or like_count 0)
         :fb-comment-count comment_count
         :fb-click-count (or click_count 0)}
        )
      (recur board id))
    )
  )