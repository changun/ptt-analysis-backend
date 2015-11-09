(ns ptt-analysis.sentiment
  (:require [ptt-analysis.post :as post]
            [cheshire.core :as json]
            [clojure.core.reducers :as r])
  (:import (java.io File)
           (edu.stanford.nlp.ie.crf CRFClassifier)
           (java.util Properties)
           (com.luhuiguo.chinese ChineseUtils)))


(def training
  (with-open [wrt (clojure.java.io/writer "sentiment_samples.json")]

    (->> (file-seq (File. "/data/pttrocks"))
         (filter #(re-matches #".*\.html$" (.getName ^File %)))
         (mapcat (comp :pushed post/html->post slurp))
         (map #(vector (:op %) (clojure.string/replace (:content %) #"[^\p{L}？！?!]" " ") ))
         (#(json/generate-stream % wrt))
         ))
  )



(def segmenter
  (let [prop (doto (Properties.)
               (.setProperty "sighanCorporaDict" "/data/ptt-analysis-backend/stanford-segmenter-2015-04-20/data")
               (.setProperty "NormalizationTable" "/data/ptt-analysis-backend/stanford-segmenter-2015-04-20/data/norm.simp.utf8")
               (.setProperty "normTableEncoding" "UTF-8")
               (.setProperty "serDictionary" "/data/ptt-analysis-backend/stanford-segmenter-2015-04-20/dict.ser.gz"))

        ]
    (doto (CRFClassifier. prop)
      (.loadClassifierNoExceptions "/data/ptt-analysis-backend/stanford-segmenter-2015-04-20/data/ctb.gz" prop))

    ))

(with-open [wrtr (clojure.java.io/writer "content.txt")]
  (doseq [[ line-num line] (->> (file-seq (File. "/data/pttrocks"))
                    (filter #(re-matches #".*\.html$" (.getName ^File %)))
                    (pmap (comp :content post/html->post slurp))
                    (map #(clojure.string/replace % #"([^\n])\n([^\n])" "$1$2"))
                    (map #(clojure.string/replace % #"[^\p{L}？！?!]+" " "))
                    (map #(ChineseUtils/toSimplified %))
                    (pmap #(.segmentString segmenter %))
                    (map #(filter seq %))
                    (map #(clojure.string/join " " %))
                    (map vector (range))



                    )]
    (.write wrtr line)
    (.write wrtr "\n")
    (if (= (mod line-num 10000) 0)
      (println line-num)
      )
    )
  )

(with-open [pushes (clojure.java.io/writer "pushes.txt")
            dislikes (clojure.java.io/writer "dislikes.txt")
            neutral (clojure.java.io/writer "neutral.txt")
            wrtr (clojure.java.io/writer "content.txt")
            ]

  (doseq [[num {:keys [pushed content]}] (->>
                                           (file-seq (File. "/data/ptt-data"))
                                (filter #(re-matches #".*\.html$" (.getName ^File %)))
                                (pmap (comp post/html->post slurp))
                                (map vector (range))
                                )
          ]
    (if content
      (let [main-content (->  (clojure.string/replace content #"([^\n])\n([^\n])" "$1$2")
                              (clojure.string/replace  #"[^\p{L}？！?!]+" " ")
                              (ChineseUtils/toSimplified )
                              (->> (.segmentString segmenter)
                                   (filter seq)
                                   (clojure.string/join " "))

                              ) ]
        (.write wrtr main-content)
        (.write wrtr "\n")

        ))
    (doseq [{:keys [op content]} pushed]
      (if content
        (let [out (condp = op
                    "推" pushes
                    "噓" dislikes
                    "→" neutral
                    nil)
              line (-> content
                       (clojure.string/replace  #"[^\p{L}？！?!]+" " ")
                       (ChineseUtils/toSimplified)
                       (->>
                         (.segmentString segmenter)
                         (filter seq)
                         (clojure.string/join " ")
                         )
                       )]
          (when (and (seq (clojure.string/trim line)) out)
            (.write out line)
            (.write out "\n"))

          )))
    (if (= (mod num 10000) 0)
      (println num)
      )

    )
  )



(with-open [out (clojure.java.io/writer "wiki-lines.txt")]
  (doseq [content (->> (file-seq (File. "/data/ptt-analysis-backend/wiki/text/"))
                       (filter #(re-matches #"wiki.*" (.getName ^File %)))
                       (map slurp)
                       (mapcat #(clojure.string/split % #"(</doc>\n)?<doc id=\"\d+\" url=\"https://zh.wikipedia.org/wiki\?curid=\d+\" title=\"[^\"]+\">"))
                       (filter seq)
                       (map #(.substring % (.indexOf % "\n" 1)) )


                       )]
    (let [line (-> content
                   (clojure.string/replace  #"[^\p{L}？！?!]+" " ")
                   (clojure.string/replace  #"\s+" " ")
                   (ChineseUtils/toSimplified)
                   (->>
                     (.segmentString segmenter)
                     (filter seq)
                     (clojure.string/join " ")
                     )
                   )]
      (when (and (seq (clojure.string/trim line)))
        (.write out line)
        (.write out "\n")
        )

      )
    )
  )