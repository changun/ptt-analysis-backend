(ns ptt-analysis.async
  (:require [clojure.core.async :refer [go <!]]))

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
