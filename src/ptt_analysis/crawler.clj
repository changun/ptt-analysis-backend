(ns ptt-analysis.crawler
  (:import (org.joda.time DateTimeZone)
           (java.util.concurrent  LinkedBlockingQueue))
  (:require
    [clojure.core.async :refer [thread]]
    [ptt-analysis.endpoint :refer [base-path]]
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
        [clojure.data.priority-map]))





(DateTimeZone/setDefault (DateTimeZone/forID "Asia/Taipei"))

; task queue

(def task-queue (LinkedBlockingQueue. 200000))
(defn get-task! [] (.take task-queue))
(defn add-task! [task] (.add task-queue task))


; task generator
(defn post-filter [{:keys [id]}]
  (not (t/before?
         (post/id->time id)
         (t/minus (t/now) (t/weeks 4)))))


(defn scan-posts [board]
  (->> (ptt/post-ids board)
       (take-while post-filter)
       )
  )

(defn get-boards []
  (into #{} (concat ptt/default-boards (ptt/hot-boards) (solr/boards)))
  )



; task worker


(def finished (atom 0))
(def unchanged (atom 0))
(def route-usage (atom {}))
(def errors (atom #{}))

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

(defn exist? [task body status]
  (or
    (= 304 status)
    (let [{:keys [string-hash title author]} (solr/get-post task)]
      (and
        title author (= (post/post-hash body) string-hash)))

    )
  )

(defn pipeline [route]
  (binding [base-path route]
    (let [task (get-task!)]
      (try
        (let [{:keys [body status]}
              (ptt/get-raw-post task)
              ]
          (cond
            (= status 404)
            (trace (str task) " removed")
            (exist? task body status)
                (swap! unchanged inc)
                :default
                (->
                  task
                  (assoc :raw-body body :string-hash (post/post-hash body))
                  (post/html->post)
                  (doto (s3/add-post))
                  (doto (solr/add-post))
                  )
                )
          (swap! finished inc)
          (swap! route-usage #(assoc % route (inc (or (get @route-usage route) 0))))
          )
        (catch Throwable e
          (swap! errors conj (assoc task :error e)))))

    )
  (if (not (Thread/interrupted))
    (recur route))
)

(defn generator []
  (loop []
    (try
      (doseq [board (shuffle (get-boards))]
        (try
          (doseq [post (scan-posts board)]
            (add-task! post))
          (catch Throwable _))
        )
      (catch Throwable e (clojure.stacktrace/print-cause-trace e))
      )
    (if (not (Thread/interrupted))
      (recur))
    )
  )



(defn reporter []
  (loop []
    (println "==================================================")
    (println (format "Finished %d Unchanged %d Error %d"
                     @finished @unchanged (count @errors)))
    (println "==================================================")
    (doseq [e @errors]
      (println "=================ERROR============================")
      (println e)
      )
    (println "==================================================")
    (doseq [u @route-usage]
      (println u)
      )
    (reset! finished 0)
    (reset! unchanged 0)
    (reset! errors #{})
    (reset! route-usage {})

    (if (not (Thread/interrupted))
      (do
        (Thread/sleep 60000)
        (recur)))
    )

  )


(defn create-crawler []
  {:workers
   (doall
     (for [route routes]
       (future (pipeline route))
       ))
   :reporter (future (reporter))
   :generator (future (generator))
   }
  )

(defn stop [{:keys [workers reporter generator]}]
  (doseq [w (conj workers reporter generator)]
    (future-cancel w)
    )

  )