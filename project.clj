(defproject ptt-analysis "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :jvm-opts ["-Xmx1g" "-server" "-XX:-OmitStackTraceInFastThrow"]
  :profiles {:dev {:resource-paths ["resources"]}}
  ;:aot [ptt-analysis.main]
  ; :main ptt-analysis.main
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [http-kit "2.1.16"]
                 [enlive "1.1.5"]
                 [clj-time "0.8.0"]
                 [com.novemberain/monger "2.0.0"]
                 [compojure "1.2.1"]
                 [ring "1.3.1"]
                 [ring/ring-json "0.3.1"]
                 [ring-cors "0.1.4"]
                 [com.taoensso/timbre "3.3.1"]
                 [com.luhuiguo/chinese-utils "1.0"]
                 [amazonica "0.3.22"]
                 [log4j/log4j "1.2.17"]
                 [swiss-arrows "1.0.0"]
                 [prismatic/schema "1.0.4"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [clj-http "2.2.0"]
                  [org.littleshoot/littleproxy "1.1.0"]

                 ])

