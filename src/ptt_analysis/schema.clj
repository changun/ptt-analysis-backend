(ns ptt-analysis.schema
  (:require [schema.core :as s]
            [clj-time.core :as t])
  )

(def Push
  {:op      (s/enum "→" "推" "噓")
   :user    s/Str
   :time    (s/protocol t/DateTimeProtocol)
   :content s/Str}
   )

(def Post
  {:id (s/pred #(re-matches #"M\.\d+\.[\w]\.[\d\w]+" %))
   :board s/Str
   :author s/Str
   :title s/Str
   :time (s/maybe (s/protocol t/DateTimeProtocol))
   :pushed [Push]
   :ip s/Str
   :content s/Str
   :length s/Int
   :content-links [s/Str]
   :push-links  [s/Str]
   :raw-body s/Str
   })

(def FaceStats
  {:fb-share-count s/Int
   :fb-like-count s/Int
   :fb-comment-count s/Int
   :fb-click-count s/Int
   }
  )
(def SolrPost
  (merge
    FaceStats

    {:id               (s/pred #(re-matches #".+:M\.\d+\.[\w]\.[\d\w]+" %))
     :content [s/Str]
     :length s/Int
     :popularity       s/Int
     :push             s/Int
     :dislike          s/Int
     :arrow            s/Int
     :isReply          s/Bool
     :last_modified    s/Str
     :string-hash      s/Int
     :category         s/Str
     :content-links [s/Str]
     :push-links  [s/Str]
     :title s/Str
     :author s/Str
     })
  )
