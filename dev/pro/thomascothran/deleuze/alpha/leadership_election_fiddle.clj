(ns pro.thomascothran.deleuze.alpha.leadership-election-fiddle
  (:require [clojure.pprint :as pp]
            [pro.thomascothran.deleuze.alpha.curator :as cu]
            [pro.thomascothran.deleuze.alpha.leadership-election
             :as le]))


(def -zk-string
  "localhost:2181,localhost:2182")

(defonce -client
  (atom nil))
(defonce -latch
  (atom nil))

(defn -start-client
  ([] (-start-client {:zookeeper/connection-string -zk-string
                ::cu/max-retries 2}))
  ([config]
   (when @-client
     (throw (ex-info "Client already exists"
                     {:client @-client})))
   (let [client (cu/client config)]
     (reset! -client client)
     client)))

(defn -stop-client
  ([] (-stop-client -client))
  ([client-atom]
   (.close @client-atom)
   (reset! -client nil)))

(defn -start-latch
  ([] (-start-latch {:curator-framework/client @-client
                     :curator.leadership/path "/test/leadership"
                     :curator.participant/id "a"
                     :curator.leadership/on-leadership
                     #(println "Acquired leadership")
                     :curator.leadership/on-lost-leadership
                     #(println "Lost leadership")}))
  ([latch-opts]
   (let [latch (le/latch latch-opts)]
     (reset! -latch latch)
     (pp/pprint  {:msg "Started latch"
                  :latch @latch
                  :latch-info (le/inspect latch)})
     (Thread/sleep 55000)

     (pp/pprint  {:msg "Closing latch"
                  :latch @latch
                  :latch-info (le/inspect latch)})
     ((:curator.latch/close! @latch))
     (println "Latch closed"))))

(comment
  (do (-start-client)
      (println "Started client")
      (-start-latch)
      (-stop-client)
      (println "Stopped client")))
