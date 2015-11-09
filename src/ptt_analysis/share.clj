

(ns ptt-analysis.share
  (:require [amazonica.aws.s3 :as s3]
            [net.cgrand.enlive-html :as html]))
(def cred (delay (clojure.edn/read-string (slurp "aws.cred"))))


(defn get-page-froms3 [board post-id]
  (-> (s3/get-object
        @cred
        {:bucket-name "ptt.rocks"
         :key (str "posts/" board "/" post-id) }
        )
      :object-content
      (slurp)
      (html/html-snippet)

      ))

(html/deftemplate share-page-template (File. "/home/azureuser/share/index.html")
                  [ctxt]
                  [:#main-content] (html/content (:main ctxt))
                  [:head :title] (html/content (:title ctxt))
                  )





(defn share-page [board post-id req]
  (let [ page (get-page-froms3 board post-id)]
    {:body (apply str (share-page-template
                        {:main (html/select page [:#main-content])
                         :title (apply str "看看鄉民怎麼說 - " (html/texts (html/select page [:head :title])))
                         }))})

    )

