(ns ptt-analysis.async
  (:require
    [org.httpkit.client :as http]
    [clojure.core.async :refer [go <! chan close! put!]]))

(defmacro go-try [& clauses]
  (let [s (gensym)]
    `(go (try ~@clauses (catch Exception ~s ~s)))
    ))
(defn error? [%] (instance? Exception %))

(defmacro <? [clause]
  (let [s (gensym)]
    `(let [~s (<! ~clause) ]
       (if (error? ~s)
         (throw ~s)
         ~s)
       )))

(defn async-post[& args]
  (let [ch (chan 1)]
    (apply http/post (concat  args [#(if (or (:error %) (= (:status %) 503))
                                      (do (put! ch (Exception. (str "Query " args "Error:" %)))
                                          (close! ch))
                                      (do (put! ch (:body %))
                                          (close! ch)))]))
    ch
    )
  )
(defn async-get [& args]
  (let [ch (chan 1)]
    (apply http/get (concat  args [#(if (or (:error %) (= (:status %) 503))
                                     (do (put! ch (Exception. (str "Query " args "Error:" %)))
                                         (close! ch))
                                     (do (put! ch (:body %))
                                         (close! ch)))]))
    ch
    )
  )
