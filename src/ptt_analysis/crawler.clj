(ns ptt-analysis.crawler
  (:import (org.joda.time DateTimeZone))
  (:require
    [clojure.core.async :refer [thread]]
    [taoensso.timbre
     :refer (log info warn error trace)]
    [taoensso.timbre.profiling :refer (pspy pspy* profile defnp p p*)]
    [ptt-analysis.solr :as solr]
    [ptt-analysis.post :as post]
    [ptt-analysis.ptt :as ptt]
    [ptt-analysis.s3 :as s3]
    [clojure.edn]
    [clj-time.core :as t])

  (:use
        [ptt-analysis.schema]
        [ptt-analysis.async]
        [ptt-analysis.endpoint]
        [ptt-analysis.workqueue]
        [clojure.data.priority-map]))



(def routes
  [""
   "https://civil-accord-94016.appspot.com/proxy?url="
   "https://ptt-rocks-proxy.appspot.com/proxy?url="
   "https://spiritual-grin-94018.appspot.com/proxy?url="
   "https://coastal-height-94118.appspot.com/proxy?url="
   "https://proxy-962.appspot.com/proxy?url="
   "https://proxy-425.appspot.com/proxy?url="
   "https://proxy-426.appspot.com/proxy?url="
   "https://proxy-427.appspot.com/proxy?url="
   "https://proxy-428.appspot.com/proxy?url="
   "https://proxy-429.appspot.com/proxy?url="
   "https://proxy-430.appspot.com/proxy?url="])

(DateTimeZone/setDefault (DateTimeZone/forID "Asia/Taipei"))

(declare run get-post check-update upload-to-solr upload-to-s3 schedule parse-post get-post-raw)

(def agents (set (for [route routes
                       id [0 1]]
                   (agent {::t #'get-post-raw :route route :id id})
                   )))

(defn get-post-raw
  [{:keys [route] :as state}]
  (try
    (let [{:keys [id board string-hash] :as task} (take!)]
      (println task)
      (binding [ptt-analysis.endpoint/base-path route]
        (let [; get string hash
              string-hash (or string-hash (:string-hash (solr/get-post board id)))
              ; make request
              {:keys [body status]} @(ptt/post-raw board id string-hash)
              ; assoc string hash
              task (assoc task :string-hash (post/post-hash body) )
              ]
          (cond
            (and (= status 200) body)
            (merge state
                   {:post-raw body
                    :task task
                    ::t #'parse-post})
            (= status 304)
            (do (println "String hash worked")
                (merge state
                       {:task task
                        ::t #'schedule}))
            ; Unmodified

            (= status 404)
            state
            :default
            (do (error status body)
                (merge state
                       {:task task
                        ::t #'schedule}))
            )
          ))
      )
    (catch Exception e
      (error e "get-post-raw")
      ;; skip any URL we failed to load
      state)
    (finally (run *agent*)))
  )

(defn parse-post
  [{:keys [post-raw task] :as state} ]
  (try
    (let [ {:keys [board id]} task]

      (if-let [post (post/html->post board id post-raw)]
        (merge state
               {:post post
                ::t #'schedule})
        (throw (RuntimeException. "Can't parse" post-raw))
        ))

    (catch Exception e
      (error e task)
      (merge state
             {::t #'schedule}))
    (finally (run *agent*)))
  )
(defn check-update
  [{:keys [post] :as state}]
  (try
    (if (not= (post/post-hash (:raw-post post)) (:string-hash (solr/get-post (:board post) (:id post))))
      (merge state {::t #'upload-to-solr})
      (merge state {::t #'schedule})
      )
    (catch Exception e
      ;; retry
      state)
    (finally (run *agent*))))

(defn upload-to-solr
  [{:keys [post] :as state}]
  (try
    (solr/add-post post)
    (merge state {::t #'upload-to-s3})
    (catch Exception e
      (error e "upload to solr")
      ;; retry
      state)
    (finally (run *agent*))))


(defn upload-to-s3
  [{:keys [post] :as state}]
  (try
    (s3/add-post post)
    (merge state {::t #'schedule})
    (catch Exception e
      ;; retry
      state)
    (finally (run *agent*))))

(defn schedule
  [{:keys [post task] :as state}]
  (println "Schedule")
  (try
    (let [last-update (or (:time (last (:pushed post)))
                          (:time post)
                          (t/minus (t/now) (t/minutes 30))
                          )
          schedule (t/plus (t/now) (t/seconds (t/in-seconds (t/interval last-update (t/now)))))]
      (put! task schedule)
      )

    (merge state {::t #'get-post-raw})
    (catch Exception e
      ;; retry
      (error "We should not fail!" e )
      state)
    (finally (run *agent*))))

(defn paused? [agent] (::paused (meta agent)))

(def non-blocking? #{#'upload-to-s3 parse-post})

(defn run
  ([] (doseq [a agents] (run a)))
  ([a]
   (when (agents a)
     (send a (fn [{transition ::t :as state}]
               (println transition)
               (when-not (paused? *agent*)
                 (if (non-blocking? transition)
                   (send *agent* transition)
                   (send-off *agent* transition))
                 )
               state)))))

(defn pause
  ([] (doseq [a agents] (pause a)))
  ([a] (alter-meta! a assoc ::paused true)))

(defn restart
  ([] (doseq [a agents] (restart a)))
  ([a]
   (alter-meta! a dissoc ::paused)
   (run a)))



(defn get-boards []
  (into #{} (concat ptt/default-boards (ptt/hot-boards) (solr/boards)))
  )

(defn post-filter [board id]
  (and (not (exists? (str board ":" id)))
       (not (t/before? (post/id->time id) (t/minus (t/now) (t/weeks 4))))
       ))

(defn scan-posts [board]
  (doseq [id (->> (ptt/post-ids board)
                  (filter (partial post-filter board))
                  )
          ]
    (put! {:board board :id id :_id (str board ":" id)}
          (t/plus (t/now) (t/seconds (t/in-seconds (t/interval (post/id->time id) (t/now))))))
    )
  )



