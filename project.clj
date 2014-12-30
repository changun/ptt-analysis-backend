(defproject ptt-analysis "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :jvm-opts ["-Xmx1g" "-server" "-XX:-OmitStackTraceInFastThrow"]

  ;:aot [ptt-analysis.server]
  :main ptt-analysis.crawler
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [http-kit "2.1.16"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [enlive "1.1.5"]
                 [clj-time "0.8.0"]
                 [com.novemberain/monger "2.0.0"]
                 [compojure "1.2.1"]
                 [ring "1.3.1"]
                 [ring/ring-json "0.3.1"]
                 [overtone/at-at "1.2.0"]
                 [ring-cors "0.1.4"]
                 [com.taoensso/timbre "3.3.1"]
                 [korma "0.3.0"]
                 [org.postgresql/postgresql "9.2-1002-jdbc4"]
                 [com.luhuiguo/chinese-utils "1.0"]
                 [amazonica "0.3.6"]
                 [org.clojure/tools.nrepl "0.2.5"]
                 [com.taoensso/carmine "2.4.0"]
                 ])

