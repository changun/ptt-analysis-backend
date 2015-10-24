(ns ptt-analysis.main
  (:gen-class)
  (:require [ptt-analysis.server :as server]
            [ptt-analysis.crawler :as crawler]
            )
  (:use [clojure.tools.nrepl.server :only (start-server stop-server)])
  )



(defn -main
  [& args]
  ; repl

  (let [args (into #{} args)]
    (if (contains? args "crawler")
      (do (println "start crawler")
          (start-server :port 9098)
          (crawler/start))
      )
    (if (contains? args "server")
      (do (println "start server at port 9098")
          (start-server :port 9099)
          (server/start 8089)))
    )
  (println "sleep forever...")
  (loop []
    (Thread/sleep 10000)
    (recur))

  )


