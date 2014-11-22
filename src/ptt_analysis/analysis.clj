(ns ptt-analysis.analysis
  (:require [monger.core :as mg]
            [monger.collection :as mc]))


(defn add-in [m ks val]
  (assoc-in m ks (conj (or (get-in m ks) #{}) val))
  )
(defn active? [[user m]]
  (or (> (count (:push m)) 4) (> (count (:dislike m)) 4) (> (count (:post m)) 0))
  )
(def user-m (let [conn (mg/connect)
                  db   (mg/get-db conn "ptt-analysis")
                  coll "posts"
                  posts (mc/find-maps db coll)
                  ]
              (reduce (fn [m {:keys [author pushed _id]}]
                        (let [author (keyword author)
                              _id (keyword _id)
                              m (add-in m [author :post] _id)]
                          (reduce  #(let [op (condp = (:op %2) "推" :push "噓" :dislike "→"  :arrow)]
                                     (add-in %1 [(keyword (:user %2)) op] _id)) m pushed)
                          )
                        ) {} posts)))


(defn inter [& ks]
  (apply clojure.set/intersection ks))
(defn score [u1 u2 r & {:keys [print?]}]
  (let [u1 (keyword u1)
        u2 (keyword u2)
        u1-push (get-in r [u1 :push])
        u1-dislike (get-in r [u1 :dislike])
        u1-arrow (get-in r [u1 :arrow])
        u2-push (get-in r [u2 :push])
        u2-dislike (get-in r [u2 :dislike])
        u2-arrow (get-in r [u2 :arrow])
        u1-post   (get-in r [u1 :post])
        u2-post   (get-in r [u2 :post])
        dislike (+ (count (inter u1-post u2-dislike))
                   (count (inter u1-dislike u2-post)))
        like (+ (count (inter u1-post u2-push))
                (count (inter u1-push u2-post)))

        co-push (count (inter u1-push u2-push))
        co-dislike (count (inter u2-dislike u1-dislike))
        conflict (+ (count (inter u1-dislike u2-push))
                    (count (inter u1-push u2-dislike))
                    (* 3 dislike))
        u1-activities (+ (count u1-push) (count u1-dislike)  (count u1-arrow) (* 3 (count u1-post)))
        u2-activities (+ (count u2-push) (count u2-dislike) (count u2-arrow) (* 3 (count u2-post)))]
    (if print?
      (do
        (println (format "%s Post %d Push %d Dislike %d" u1 (count u1-post) (count u1-push) (count u1-dislike) ))
        (println (format "%s Post %d Push %d Dislike %d" u2 (count u2-post) (count u2-push) (count u2-dislike) ))
        (println (format "Co-push %s Co-dislike %s Conflict %s Like %s Dislike %s" co-push co-dislike conflict like dislike))))
    (/ (- (+ co-push co-dislike (* 3 like)) conflict) (/ (+ u1-activities u2-activities) 2))
    )
  )

(defn first-n [n target]
  (take n (reverse
            (sort-by second
                     (for [user (keys user-m)]
                       [user (score target user user-m)] ))))
  )
(defn last-n [n target]
  (take n (sort-by second
                   (for [user (keys user-m)]
                     [user (score target user user-m)] )))
  )


(def post->user (let [conn (mg/connect)
                      db   (mg/get-db conn "ptt-analysis")
                      coll "posts"
                      posts (mc/find-maps db coll)
                      ]
                  (into {} (for [post posts]
                             [(keyword (:_id post)) (keyword (:author post))]))
                  ))
(defn ever-push? [user] (seq (get-in user-m [user :push])))

(def user->op->user
  (into {} (filter identity (for [[user m] user-m]
                              (if (seq (:push m))
                                [user {:push (frequencies (filter ever-push? (map post->user (:push m))))
                                       :dislike (frequencies (filter ever-push? (map post->user (:dislike m))))}]
                                nil)
                              )))
  )
(def init-rank 100)
(def ranks (into {} (map #(identity [% init-rank]) (keys user->op->user))))

(def page-rank (loop [ranks ranks runs 50]
                 (println "Runs " runs " score " (:XXXXGAY ranks) " sum:" (apply + (map second ranks)))
                 (if (> runs 0)
                   (recur
                     (time (let [maps (for [[user user-ops] user->op->user]
                                        (let [score (user ranks)
                                              portion (/ (* 0.85 score) (apply + (map second (:push user-ops)) ))]
                                          (conj (for [[pushed-user push-times] (:push user-ops)]
                                                  [pushed-user (* push-times portion)]
                                                  )
                                                [user (* 0.15 score)])

                                          )
                                        )
                                 maps (apply concat maps)
                                 groups (group-by first maps)
                                 reduce-ret (into {} (map (fn [[user scores]]  [user (apply + (map second scores))]) groups))]
                                reduce-ret
                             )
                           )
                     (- runs 1))
                   ranks)))