(ns ptt-analysis.solr
  (:require [cheshire.core :as json]
            [org.httpkit.client :as http]))

(defn solr-endpoint [method path & [options]]
  @(http/request (-> options
                     (assoc :method method)
                     (assoc :headers (assoc (:headers options)"Content-Type" "application/json"))
                     (assoc :url (str "http://localhost:8983/solr/collection1" path))
                     )
                 identity
                 )
  )

(defn add-posts [posts]
  (solr-endpoint :post "/update?wt=json&commitWithin=60000"
              {:body    (json/generate-string posts)}))
(defn get-post [board id]
  (-> (solr-endpoint :get (format "/select?q=id:%%22%s:%s%%22&wt=json" board id)
                     )
      :body
      (json/parse-string true)
      :response
      :docs
      first)
  )


(defn optimize []
  (solr-endpoint :post "/update?optimize=true"))

(defn get-all-boards []
  (-> (solr-endpoint :get "/select?q=*%3A*&rows=0&wt=json&indent=true&facet=true&facet.field=board")
      :body
      (json/parse-string true)
      (get-in [:facet_counts :facet_fields :board])
      (->> (partition 2)
           (map (partial apply vector))
           (into {})
           )
      (keys)
      ))
