(ns ptt-analysis.db
  (:import (org.joda.time DateTimeZone DateTime))
  (:use [korma.db]
        [korma.core])
  (:require [clj-time.coerce :as c]
            [monger.core :as mg]
            [monger.collection :as mc]
            monger.joda-time
            [taoensso.timbre :as timbre
             :refer (log trace debug info warn error fatal report
                         logf tracef debugf infof warnf errorf fatalf reportf
                         spy logged-future with-log-level with-logging-config
                         sometimes)]
            [clj-time.core :as t])
  (:import (org.postgresql.util PGobject)))
(DateTimeZone/setDefault (DateTimeZone/forID "Asia/Taipei"))
(defdb prod (postgres {:db "ptt"
                       :user "ptt"
                       :password "ptt"}))


(defn str->pgobject
  [type value]
  (doto (PGobject.)
    (.setType type)
    (.setValue value)))
(defn keyword->pgobject
  [type value]
  (doto (PGobject.)
    (.setType type)
    (.setValue (name value))))

(defn op->sql  [op]
  (keyword->pgobject "ptt_operation" op)
  )
(defn str->push-time [post-dt push-date]

  (try
    (let [[_ m d h mi] (re-find #"(\d\d)/(\d\d) (\d+):(\d+)?"  push-date)

          ]
      (DateTime. (t/year post-dt)
                 (Integer/parseInt m)
                 (Integer/parseInt d)
                 (Integer/parseInt h)
                 (Integer/parseInt mi)
                 )
      )
    (catch Exception e (do (error e push-date)
                           nil)))
  )


(declare user action post)
(defentity user
           (pk :id)
           (table (keyword "user"))
           (has-many action)
           (has-many post)
           )
(defentity post
           (pk :id)
           (belongs-to user)
           (has-many action)
           (transform (fn   [{time :time :as post}]
                        (cond-> post
                                time
                          (assoc :time (c/from-sql-time time))
                          )))
           (prepare (fn   [{time :time :as post}]
                      (assoc post :time (c/to-sql-time time))))
           )
(defentity action

           (entity-fields :op :post_id :user_id :board)
           (belongs-to user)
           (belongs-to post {:fk :post_id})

           (transform (fn   [{time :time  op :op :as act}]
                        (cond-> act
                             time
                             (assoc :time (c/from-sql-time time)
                                    :op (keyword (.getValue op))))))
           (prepare (fn   [{time :time  op :op :as act}]
                      (assoc act :time (c/to-sql-time time)
                                 :op (str->pgobject "ptt_operation" (name op)))))
           )



(defn save-post [{:keys [_id author pushed time title board] :as p}]


  (if (and author time title board)
    (try
      (if-not (seq (select post (where {:id _id :board board})))
        (insert post (values {:id _id :user_id author :time (c/to-sql-time time) :subject title :board board}))
        )
      (if-not (seq (select user (where {:id author})))
        (insert user (values {:id author}))
        )
      (let [actions (for [{:keys [op user ip-date content]} pushed]
                      {:user_id user
                       :op (condp = op "推" :push "噓" :dislike "→"  :arrow)
                       :post_id _id
                       :content content
                       :board board
                       :time (c/to-sql-time (str->push-time time ip-date))}
                      )
            actions (into #{} (filter :time actions))
            ]

        (when (seq actions)
          (doseq [{:keys [user_id] :as act} actions]
            (if-not (seq (select action (where (-> act
                                                   (assoc :op (op->sql (:op act)))
                                                   (dissoc  :content)) )))
              (insert action (values act))
              )
            (if-not (seq (select user (where {:id user_id})))
              (insert user (values {:id user_id}))
              )
            ))
        )
      p
      (catch Exception e (do (error e)
                             (clojure.pprint/pprint p)
                             (throw e)))

      )
    (error "Missing Required Field" p)
    )
  )

(defn refresh-action-detail-table []
  (exec-raw "REFRESH MATERIALIZED VIEW action_detail;"))

(defn get-max-time-for-boards []
  (let [ret (exec-raw ["SELECT board , MAX(\"time\") FROM post group by board"] :results)]
    (into {} (map #(identity [(clojure.string/trim (:board %)) (c/from-sql-time (:max %))]) ret))

    )
  )

(defn get-post-count-for-boards []
  (let [ret (exec-raw ["select board, count(*) AS count from post group by board order by count desc;"] :results)]
    (into {} (map #(identity [(:board %) (:count %)]) ret))
    )
  )
(defn get-pairwise-push-count [& {:keys [board after before]}]

  (let [after (if after (c/to-sql-time after))
        before (if before (c/to-sql-time before))
        query (format
                "     SELECT     \"user\",
                                 author,
                                 Count(*)       AS count
                      FROM       action_detail
                      WHERE      op='push' %s %s %s
                      GROUP BY   \"user\", author"
                (if board "and board=?" "")
                (if after "and time >= ?" "")
                (if before "and time < ?" "")
                )
        params (filter identity [board after before])
        ]
    (exec-raw  [query params]
      :results)
    )


  )
(defn group-by-user [ret]
  (let [ret (group-by first
                      (map (fn [{:keys [post user] }]
                             [user post]) ret))]
  (into {}     (map (fn [[user u-posts]]
                      [user (into #{} (map second u-posts))]
                      ) ret)))


  )
(defn get-user-op [op]
  (let [ret (exec-raw
              (format
               "SELECT     user_id AS user,
                          post_id AS post
               FROM       action
               WHERE      op='%s'" (name op))
               :results)

        ]
    (group-by-user ret)))
(defn get-user-posts []
  (let [ret (exec-raw
              "SELECT     user_id AS user,
                          id AS post
               FROM       post"
              :results)

        ]
    (group-by-user ret)))

