(ns ptt-analysis.server
  (:import (org.joda.time DateTime DateTimeZone))
  (:use org.httpkit.server)
  (:use ptt-analysis.analysis)
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [ring.middleware.cors :refer [wrap-cors]]
            [clj-time.core :as t]
            [ptt-analysis.db :as db]
            [taoensso.timbre :as timbre
             :refer (log info warn error)]
            [taoensso.timbre.profiling :as profiling
             :refer (pspy pspy* profile defnp p p*)])
  (:use [compojure.route :only [files not-found]]
        [compojure.handler :only [site]] ; form, query params decode; cookie; session, etc
        [compojure.core :only [defroutes GET POST DELETE ANY context]]
        org.httpkit.server)
  (:use [ring.middleware.params         :only [wrap-params]]
        [ring.middleware.json :only [wrap-json-response]]
        )
  (:use overtone.at-at)
  (:use [ptt-analysis.crawler :only [fetch-posts]]))
(DateTimeZone/setDefault (DateTimeZone/forID "Asia/Taipei"))

(defonce dat (atom {
                    :ranks []
                    :user->pushes {}
                    :user->posts {}
                    }))
(defonce refresh-pool (mk-pool))

(defn similarity [req]          ;; ordinary clojure function
  (let [user-x (-> req :params :user-x)
        user-y (-> req :params :user-y)]
    {:body (score user-x user-y @dat)}
    ))


(defn users-similarity-rank [req & {:keys [desc?]}]
  (let [target (keyword (-> req :params :target))
        n (. Integer parseInt (-> req :params :n))
        users (filter #(not= target %) (keys @dat))
        ranks (sort-by second
                       (pmap #(identity [% (:similarity (score target % @dat))]) users))]
    {:body
     (take n (cond-> ranks desc? (reverse))
           )}
    )
  )

(defn show-user-page-rank [req]
  {:body
   (take (. Integer parseInt (-> req :params :n))
           (reverse (sort-by second @user-page-rank))
         )}
  )
(defn most-similar [req]
  (users-similarity-rank req :desc? true)
  )
(defn most-different [req]
  (users-similarity-rank req :desc? false)
  )
(defn top-ranks [& params] (profile :info (keyword (str params)) (take 200 (apply compute-page-rank 1 params))))
(defn refresh-data []
    (let [
          today (.withTimeAtStartOfDay (DateTime.))
          this-week (.withDayOfWeek today 1)
          this-month (.withDayOfMonth today 1)
          yesterday (t/minus today (t/days 1))
          last-week (t/minus this-week (t/weeks 1))
          last-month  (t/minus this-month (t/months 1))
          boards (map first (reverse (sort-by second (db/get-post-count-for-boards))))
          ranks (doall (for [board (concat [nil] boards)]
                         {:board (or board "ALL")
                          :ranks {:today (top-ranks :board board :after today)
                                  :this-week (top-ranks :board board :after this-week)
                                  :this-month (top-ranks :board board :after this-month)
                                  :yesterday  (top-ranks :board board :after yesterday :before today)
                                  :last-week  (top-ranks :board board :after last-week :before this-week)
                                  :last-month  (top-ranks :board board :after last-month :before this-month)
                                  :overall  (if board (top-ranks :board board))
                                  }}))
          ]

      {

       :user->pushes (db/get-user-op :push)
       :user->posts (db/get-user-posts)
       :user->dislikes (db/get-user-op :dislike)
       :ranks ranks

       }
      )
  )

(defroutes all-routes
           (GET "/similar" [] most-similar)
           (GET "/dissimilar" [] most-different)
           (GET "/rank" [] show-user-page-rank)
           (route/files "/static/") ;; static file url prefix /static, in `public` folder
           (route/not-found "<p>Page not found.</p>")) ;; all other, return 404



(def handler (handler/site (wrap-cors
                             (wrap-json-response (wrap-params all-routes))
                               :access-control-allow-origin #".*"
                               :access-control-allow-methods [:get :put :post :delete]) ))


(defonce server (atom nil))

(defn stop-server []
  (when-not (nil? @server)
    ;; graceful shutdown: wait 100ms for existing requests to be finished
    ;; :timeout is optional, when no timeout, stop immediately
    (@server :timeout 100)
    (reset! server nil)))



(defn -main [& args]
  ;; The #' is useful when you want to hot-reload code
  ;; You may want to take a look: https://github.com/clojure/tools.namespace
  ;; and http://http-kit.org/migration.html#reload
  (stop-server)
  (stop-and-reset-pool! refresh-pool :strategy :kill)
  (every 3600000 (fn []
                   (time (fetch-posts (t/days 2)))
                   (println "Refresh data")
                   (time (reset! dat (get-dataset)))
                   (println "Refresh page rank")
                   (time (reset! user-page-rank (compute-page-rank @dat 100)))
                   ) refresh-pool :fixed-delay true)

  )


