(ns ptt-analysis.s3
  (:require
    [amazonica.aws.s3 :as s3]
    [clojure.edn :as edn])
  (:import (java.io ByteArrayInputStream)))

(def get-cred (delay (edn/read-string (slurp "aws.cred"))) )



(defn add-post [{:keys [id board raw-body]}]
  (let [bytes (.getBytes raw-body "UTF-8")
        input (ByteArrayInputStream. bytes)
        key (str "posts/" board "/" id ".html")
        length (alength bytes)]
    (s3/put-object @get-cred
                   :bucket-name "ptt.rocks"
                   :key key
                   :input-stream input
                   :metadata {:content-length length}
                   :return-values "ALL_OLD")
    )
  )
