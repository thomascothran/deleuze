(ns pro.thomascothran.deleuze.alpha.event-sourcing-fiddle
  (:require [clojure.pprint :as pp]
            [malli.core :as mi]
            [next.jdbc :as jdbc]
            [pro.thomascothran.deleuze.alpha.topics :as topics]
            [pro.thomascothran.deleuze.alpha.client :as dc]
            [pro.thomascothran.deleuze.alpha.consumer
             :as consumer]
            [pro.thomascothran.deleuze.alpha.admin :as da]
            [pro.thomascothran.deleuze.alpha.event-sourcing
             :as es])
  (:import [java.util UUID]
           [java.time Instant]))

(def -admin-url
  "http://localhost:8080")
(def -service-url
  "pulsar://localhost:6650")

(defn admin-client
  []
  (da/client {:pulsar.service/admin-url -admin-url}))
(defonce -ds
  (atom nil))

(defn client
  []
  (dc/client {:pulsar.service/url -service-url}))

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

(def -event-schema
  (-> [:or
       [:map
        [:first-name string?]
        [:last-name string?]]
       [:map [:age number?]]]
      mi/schema))

(def -state-schema
  (-> [:map
       [:fullname string?]]
      mi/schema))

(def -event
  {:aggregate/id #uuid "5bc0cadf-d862-4ffc-8515-1118dc044aa6"
   :aggregate/name :cool-people
   ;; :events/schema -event-schema
   :event/id (UUID/randomUUID)
   :event/type :event.type/merge-name
   :event/occurred-at (Instant/now)
   :first-name "Thomas"
   :last-name "Cothran"})
(comment
  (mi/explain -event-schema -event))

(def -event2
  {:aggregate/id #uuid "5bc0cadf-d862-4ffc-8515-1118dc044aa6"
   :aggregate/name :cool-people
   ;; :events/schema -event-schema
   :event/id (UUID/randomUUID)
   :event/type :event.type/set-age
   :event/occurred-at (Instant/now)
   :age 34})


(def -event3
  {:aggregate/id #uuid "5bc0cadf-d862-4ffc-8515-1118dc044aa6"
   :aggregate/name :cool-people
   ;; :events/schema -event-schema
   :event/id (UUID/randomUUID)
   :event/type :event.type/set-age
   :event/occurred-at (Instant/now)
   :age 30})

(def -command-schema
  (-> [:map
       [:command/type [:enum :make-older]]
       [:new-age int?]]
      mi/schema))
(def -command
  {:aggregate/id #uuid "5bc0cadf-d862-4ffc-8515-1118dc044aa6"
   :aggregate/name :cool-people
   :command/type :make-older
   :new-age 70
   :command/issued-at (Instant/now)})

(defn -command-handler
  [_state {command-type :command/type
           age          :new-age
           :as command}]
  (case command-type
    :make-older
    (-> (select-keys command
                     [:aggregate/id :aggregate/name])
        (assoc :event/id (UUID/randomUUID)
               :event/type :event.type/set-age
               :event/occurred-at (Instant/now)
               :age age))))

(defn -reducer
  [state {event-type :event/type
          version :aggregate/version
          :keys [first-name last-name age]
          :as event}]
  (when (and (not= 0 version)
             (nil? state))
    (throw (ex-info "State is nil"
                    {:state state
                     :event event})))
  (case event-type
    :event.type/merge-name
    (assoc state :fullname (str first-name " " last-name))
    :event.type/set-age
    (assoc state :age age)))

(comment
  (with-open [ac (admin-client)
              c (client)]
    (-connect)
    (es/setup! {:datasource @-ds
                :pulsar/admin-client ac
                :tenant/name "test-tenant"})
    (let [cb (fn [{body :pulsar.message/body
                   acknowledge! :pulsar.message/acknowledge!
                   :as _msg}]
               (pp/pprint {#_#_:received-msg msg
                           :body body})
               (acknowledge!))
          consumer
          (consumer/consumer {:pulsar/client c
                              :pulsar.topic/topic "cool-people"
                              :pulsar.namespace/name
                              "deleuze_aggregate_events"
                              :pulsar.tenant/name "test-tenant"
                              :pulsar.subscription/name
                              "test-subscription"
                              :pulsar.subscription/type :exclusive})]
      (try (es/log-event! {:datasource @-ds
                           :pulsar/client c
                           :events/schema -event-schema
                           :tenant/name "test-tenant"
                           :state-schema -state-schema
                           :reducer -reducer
                           :event -event})
           (es/log-event! {:datasource @-ds
                           :pulsar/client c
                           :events/schema -event-schema
                           :state-schema -state-schema
                           :tenant/name "test-tenant"
                           :reducer -reducer
                           :event -event2})
           (es/log-event! {:datasource @-ds
                           :pulsar/client c
                           :events/schema -event-schema
                           :state-schema -state-schema
                           :tenant/name "test-tenant"
                           :reducer -reducer
                           :event -event3})
           (es/fire-command! {:datasource @-ds
                              :pulsar/client c
                              :events/schema -event-schema
                              :state-schema -state-schema
                              :tenant/name "test-tenant"
                              :reducer -reducer
                              :command -command
                              :command/schema -command-schema
                              :command/handler -command-handler})

           (future (consumer/receive-sync! {:pulsar/consumer consumer
                                            ::consumer/callback cb}))
           (Thread/sleep 50)
           (catch Exception e
             (def e e)
             (pp/pprint {:error (ex-data e)}))
           (finally (es/teardown! {:datasource @-ds
                                   :pulsar/admin-client ac
                                   :tenant/name "test-tenant"}))))
    (-disconnect)))
