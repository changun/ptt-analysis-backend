(ns ptt-analysis.main
  (:require [ptt-analysis.server :as server]
            [taoensso.timbre :as timbre
             :refer (log info warn error trace)]
            [taoensso.timbre.profiling :as profiling
             :refer (pspy pspy* profile defnp p p*)]
            [clj-time.core :as t])
  (:use [clojure.tools.nrepl.server :only (start-server stop-server)]))

(defonce server (start-server :port 9098))

(defn -main
  [& args]
  (let [args (into #{} args)]
    (info args)
    (if (contains? args "crawler")
      (do (info "start crawler")
          (ptt-analysis.crawler/start))
      )
    (if (contains? args "server")
      (do (info "start server")
          (server/start 8089)))
    )
  (info "sleep forever...")
  (loop []
    (Thread/sleep 10000)
    (recur))

  )


