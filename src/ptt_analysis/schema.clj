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
(def Title  (s/pred #(< 1 (count %) 100)) )
(def Author  (s/pred #(re-matches #"[\w\d_-]+" %)) )
(def Board  (s/pred #(re-matches #"[\w\d_-]+" %)) )

(def Task
  {:id     (s/pred #(re-matches #"M\.\d+\.[\w]\.[\d\w]+" %))
   :title (s/maybe Title)
   :author Author
   :board  Board})
(def Post
  {:id            (s/pred #(re-matches #"M\.\d+\.[\w]\.[\d\w]+" %))
   :board         Board
   :author        Author
   :title         Title
   :time          (s/protocol t/DateTimeProtocol)
   :pushed        [Push]
   :ip            s/Str
   :content       s/Str
   :length        s/Int
   :content-links [s/Str]
   :push-links    [s/Str]
   :raw-body      s/Str
   })


(def FaceStats
  {:fb-share-count s/Int
   :fb-like-count (s/maybe s/Int)
   :fb-comment-count s/Int
   :fb-click-count (s/maybe s/Int)
   }
  )

(def SolrPost
  {:id            (s/pred #(re-matches #".+:M\.\d+\.[\w]\.[\d\w]+" %))
   :content       [s/Str]
   :length        s/Int
   :popularity    s/Int
   :push          s/Int
   :dislike       s/Int
   :arrow         s/Int
   :isReply       s/Bool
   :last_modified s/Str
   :string-hash   s/Int
   :category      Board
   :content-links [s/Str]
   :push-links    [s/Str]
   :title         Title
   :author        Author
   }
  )
