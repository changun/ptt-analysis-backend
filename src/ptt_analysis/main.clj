(ns ptt-analysis.main
  (:gen-class)
  (:require [ptt-analysis.server :as server]
            [ptt-analysis.crawler :as crawler]
            [ptt-analysis.proxy :as proxy])
  (:use [clojure.tools.nrepl.server :only (start-server stop-server)])
  )



(defn -main
  [& args]
  ; repl

  (let [args (into #{} args)]
    (if (contains? args "crawler")
      (do (println "start crawler")
          (crawler/start))
      )
    (if (contains? args "server")
      (do (println "start server at port 9098")
          (server/start 8089)))
    (if (contains? args "proxy")
      (do (println "start proxy at port 9999")
          (proxy/start 9999)))
    (if (not (contains? args "proxy"))
      (start-server :port 9098))
    )
  (println "sleep forever...")
  (loop []
    (Thread/sleep 10000)
    (recur))

  )


