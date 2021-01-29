(ns pro.thomascothran.deleuze.alpha.lock-fiddle
  (:require [next.jdbc :as jdbc]
            [pro.thomascothran.deleuze.alpha.locks
             :refer [lock! #_unlock!]]))

(defonce -ds
  (atom nil))

(defn -connect
  ([] (-connect {:dbtype "postgres"
                 :dbname "default"
                 :host "localhost"
                 :user "default"
                 :password "example"
                 :port 5480}))
  ([config]
   (try (let [ds (jdbc/get-datasource config)]
          (reset! -ds ds)
          ds)
        (catch Exception e
          (println e)))))
(comment (-connect))

(defn -disconnect
  []
  (try (-> (.getConnection @-ds))
       (catch Exception e
         (println e))))
(comment (-disconnect))


(comment
  (let [p1 (lock! {:conn @-ds
                   :lock-name :test-lock3
                   :on-lock! #(println "Lock acquired")
                   :lock-type :exclusive-session})]
    p1))
