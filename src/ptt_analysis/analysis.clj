(ns ptt-analysis.analysis
  (:require [monger.core :as mg]
            [monger.collection :as mc]
            [clojure.set]
            [clojure.core.reducers :as r]
            [ptt-analysis.db :as as]
            [ptt-analysis.db :as db]
            [taoensso.timbre :as timbre
             :refer (log info warn error)]
            [taoensso.timbre.profiling :as profiling
             :refer (pspy pspy* profile defnp p p*)])
  (:use clojure.set))

(defn score [u1 u2 user->pushes user->dislike user->posts]
  (let [u1-push (user->pushes u1)
        u1-dislike (user->dislike u1)
        u2-push (user->pushes u2)
        u2-dislike (user->dislike u2)
        u1-post   (user->posts u1)
        u2-post   (user->posts u2)
        dislike (+ (count (intersection u1-post u2-dislike))
                   (count (intersection u1-dislike u2-post)))
        like (+ (count (intersection u1-post u2-push))
                (count (intersection u1-push u2-post)))

        co-push (count (intersection u1-push u2-push))
        co-dislike (count (intersection u2-dislike u1-dislike))
        conflict (+ (count (intersection u1-dislike u2-push))
                    (count (intersection u1-push u2-dislike))
                    (* 3 dislike))
        u1-activities (+ (count u1-push) (count u1-dislike) (* 3 (count u1-post)))
        u2-activities (+ (count u2-push) (count u2-dislike) (* 3 (count u2-post)))]
    (cond-> {:similarity (if (= u1 u2)
                           1
                           (/ (- (+ co-push co-dislike (* 3 like)) conflict)
                              (/ (+ 0.001 u1-activities u2-activities) 2)))
             :co-like  co-push
             :co-dislike co-dislike
             :conflict conflict
             :mutual-like like
             :mutal-dislike dislike

             u1 {:posts (count u1-post)
                 :pushes (count u1-push)
                 :dislikes (count u1-dislike)
                 }
             }
            (not= u1 u2)
              (assoc
                u2 {:posts (count u2-post)
                    :pushes (count u2-push)
                    :dislikes (count u2-dislike)
                    }))
    )
  )




(defn get-pairwise-push-distribution-without-sinks [& params]
  (let [counts (loop [counts (apply db/get-pairwise-push-count params)]
                 (let [pushers (into #{} (map :user counts))
                       ; only include those authors who have pushed other people
                       new-counts (filter #(contains? pushers (:author %)) counts)]
                   (if (= (count counts) (count new-counts))
                     counts
                     (recur new-counts)
                     )

                   )
               )
        total  (apply merge-with + (map #(identity {(:user %) (:count %)}) counts))]

    (map #(assoc % :dist (/ (float (:count %)) (total (:user %)))) counts)

    )
  )
(defn compute-page-rank [init-rank & params]
  (let [dist  (p :get-dist (apply get-pairwise-push-distribution-without-sinks params))
        users (into #{} (map :user dist))
        init-score (into {} (map #(identity [% init-rank]) users))]
    (if (> (count users) 1)
      (loop [ranks init-score]
        (let [new-ranks (p :iteration (let [outwards (map
                                               (fn [{:keys [user dist author]}]
                                                 {author (* (ranks user) dist 0.95)}
                                                 )
                                               dist)
                                    inwards (map (fn [user]
                                                   {user (* (ranks user) 0.05)})
                                                 users)
                                    ]
                                (apply merge-with + (concat outwards inwards))

                                )
                              )
              ;max-dif (apply max (map second (merge-with (fn [a b](Math/abs (- a b))) new-ranks ranks)))
              max-ratio (p :compute-max-ratio-delta (apply max (map second (merge-with (fn [a b] (/ (Math/abs (- a b)) (+ 1 b))) new-ranks ranks))))
              ]

          (if (< max-ratio 0.05)
            (reverse (sort-by second  new-ranks))
            (recur new-ranks))
          )

        )
      {}
      )
    )

  )

(defn to-map [l]
  (into {}   (map #(vector (keyword (first %)) (second %)) (partition  2 2 l))
        )
  )

(defn to-tv [tvhr]
  (into {} (map (fn [[key val]]
                  (let [val (to-map val)
                        val (assoc val :positions (map second (partition 2 (:positions val))))]
                    [key val])
                  ) (to-map (:mmseg (to-map (nth  (:termVectors tvhr) 3))))))
  )


(defn first-n-terms [tvhr fraction]
  (let [tf-idf (reverse (sort-by (comp :tf-idf second) (to-tv tvhr)))]
    (info (count tf-idf) " Terms")
    (take (* (count tf-idf) fraction) tf-idf)
    )

  )



(def post-url "http://ec2-54-189-140-251.us-west-2.compute.amazonaws.com/solr/collection1/tvrh?q=id:\"%s:%s\"&start=0&fl=mmseg&wt=json&tv.tf_idf=true&rows=1&tv.positions=true")
(defn expand-tv [tv]
  (sort-by second (mapcat (fn [[t {:keys [positions]}]] (map #(vector t %) positions) ) tv))
  )


(defn topic-extraction [board id]
  (let [tvhr (cheshire.core/parse-string (slurp (format post-url board id)) true)
        terms (first-n-terms tvhr 0.4)                                             ;(filter #(> (count (name (first %))) 1) )
        terms-in-occurence-order (expand-tv terms)
        edges (loop [[[term pos] & following-words] terms-in-occurence-order ret {}]
                (if term
                  (let [close-terms (map first (take-while (fn [[_ n-pos]] (< (- n-pos pos) 10 )) following-words))
                        new-ret (reduce
                                  (fn [ret close-term]
                                    (-> ret
                                        (assoc close-term (conj (close-term ret) term))
                                        (assoc term (conj (term ret) close-term)))
                                    ) ret close-terms) ]
                    (recur following-words new-ret)
                    )
                  ret)
                )
        dist (mapcat (fn [[term related-terms]]
                       (let [freq (frequencies related-terms)]
                         (map (fn [[t f]] [term t (/ f (count related-terms))]) freq))
                       ) edges)
        ranks (loop [ranks (into {} (map #(vector % 1) (keys edges)))]
                (let [new-ranks (p :iteration (let [outwards (map
                                                               (fn [[from to portion]]
                                                                 {to (* (ranks from) portion 0.95)}
                                                                 )

                                                               dist)
                                                    inwards (map (fn [term]
                                                                   {term (* (ranks term) 0.05)})
                                                                 (keys edges))
                                                    ]
                                                (apply merge-with + (concat outwards inwards))

                                                )
                                   )
                      ;max-dif (apply max (map second (merge-with (fn [a b](Math/abs (- a b))) new-ranks ranks)))
                      max-ratio (p :compute-max-ratio-delta (apply max (map second (merge-with (fn [a b] (/ (Math/abs (- a b)) (+ 1 b))) new-ranks ranks))))
                      ]
                  (println max-ratio)
                  (if (< max-ratio 0.01)
                    (reverse (sort-by second  new-ranks))
                    (recur new-ranks))
                  )

                )
        ranks-table (into {} ranks)
        topic-terms-ranks (filter #(> (second %) 1) ranks)
        topic-terms (into #{} (map first topic-terms-ranks))
        topic-tv (filter #(contains? topic-terms (first %)) terms)
        topic-terms-in-occurence-order (expand-tv topic-tv)
        topic-terms-in-gap (map #(vector (first %1) (if %2 (- (second %1) (second %2)) Double/POSITIVE_INFINITY))
                                topic-terms-in-occurence-order
                                (concat [nil] (butlast topic-terms-in-occurence-order))
                                )
        topic-term-ranks-with-comb
        (loop [[[term _] & following-terms] topic-terms-in-gap topic-terms-ranks topic-terms-ranks]
          (if term
            (let [[connected following] (split-with #(= 1 (second %)) following-terms)
                  connected-terms (map first connected)
                  new-ranks (if (seq connected-terms)
                              (let [new-terms (concat [term] connected-terms)
                                    new-score (apply max (for [t new-terms]
                                                           (ranks-table t)))
                                    ]
                                (conj topic-terms-ranks [new-terms new-score])
                                )
                              topic-terms-ranks
                              )]
              (recur following new-ranks)
              )
            topic-terms-ranks)
          )
        ]
    (reverse (sort-by second (into #{} topic-term-ranks-with-comb)))
    )



  )


