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

  (start-server :port 9098)
  (println "start crawler")
  (crawler/create-crawler)
  (println "start server")
  (server/start 8089)
  (println "sleep forever...")
  (loop []
    (Thread/sleep 10000)
    (recur))

  )


