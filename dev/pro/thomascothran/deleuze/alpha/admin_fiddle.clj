(ns pro.thomascothran.deleuze.alpha.admin-fiddle
  (:require [pro.thomascothran.deleuze.alpha.admin :as da]
            [pro.thomascothran.deleuze.alpha.topics :as dt]
            [pro.thomascothran.deleuze.alpha.client :as dc]
            [pro.thomascothran.deleuze.alpha.consumer
             :as consumer]
            [pro.thomascothran.deleuze.alpha.producer
             :as producer]
            [clojure.pprint :as pp]))

(def -admin-url
  "http://localhost:8080")
(def -service-url
  "pulsar://localhost:6650")

(defn client
  []
  (da/client {:pulsar.service/admin-url -admin-url}))

(comment
  (with-open [c (client)]
    (def c c)
    (pp/pprint {:clusters (da/clusters {:pulsar/admin-client c})
                :tenants (da/tenants {:pulsar/admin-client c})})
    (try (da/create-tenant! {:pulsar/admin-client c
                             :pulsar.tenant/name "test-tenant"})
         (catch Exception _
           (prn "Error creating tenant")))
    (pp/pprint {:new-tenant
                (da/tenant-info
                 {:pulsar/admin-client c
                  :pulsar.tenant/name "test-tenant"})})
    (pp/pprint {:namespaces (da/get-namespaces
                             {:pulsar/admin-client c
                              :pulsar.tenant/name "test-tenant"})})
    (try (da/create-namespace! {:pulsar/admin-client c
                            :pulsar.tenant/name "test-tenant"
                                :pulsar.namespace/name "testns"})
         (catch Exception _
           (prn "Error creating namespace")))
    (pp/pprint {:ns-auth (da/namespace-auth
                          {:pulsar/admin-client c
                           :pulsar.namespace/name "testns"
                           :pulsar.tenant/name "test-tenant"})})
    (try (with-open [dc (dc/client {:pulsar.service/url -service-url})
                     producer (dc/producer {:pulsar/client dc
                                            :pulsar.tenant/name "test-tenant"
                                            :pulsar.namespace/name "testns"
                                            :pulsar.topic/topic "test-topic"})
                     consumer (consumer/consumer
                               {:pulsar/client dc
                                :pulsar.tenant/name "test-tenant"
                                :pulsar.namespace/name "testns"
                                :pulsar.topic/topic "test-topic"
                                :pulsar.subscription/name
                                "test-subscription"
                                :pulsar.subscription/type :exclusive})]
           (producer/send! {:pulsar/producer producer
                           :pulsar.message/value "Hello world"
                           :pulsar.message/key
                           #uuid "744c98c5-8011-46d9-ad0f-68f59f5cd51d"})

           (producer/send! {:pulsar/producer producer
                           :pulsar.message/value "I love events"
                           :pulsar.message/key
                           #uuid "744c98c5-8011-46d9-ad0f-68f59f5cd51d"})
           (let [a (atom nil)
                 p (promise)
                 cb (fn [{body :pulsar.message/body
                          acknowledge! :pulsar.message/acknowledge!
                          :as msg}]
                      (pp/pprint {:received-msg msg
                                  :body body})
                      (acknowledge!)
                      (if @a
                        (do (deliver p :done!)
                            ::consumer/closed)
                          (reset! a 1)))]
             (consumer/receive-sync! {:pulsar/consumer consumer
                                      ::consumer/callback cb})
             @p))
         (catch Exception e
           (pp/pprint {:type :error
                       :error e})))

    (pp/pprint {:all-topics
                (dt/all-topics {:pulsar/admin-client c
                                :pulsar.tenant/name "test-tenant"
                                :pulsar.namespace/name "testns"})})
    (dt/delete-topic! {:pulsar/admin-client c
                       :pulsar.tenant/name "test-tenant"
                       :pulsar.namespace/name "testns"
                       :pulsar.topic/topic "test-topic"})
    ;; Cleanup
    (da/delete-namespace! {:pulsar/admin-client c
                           :pulsar.tenant/name "test-tenant"
                           :pulsar.namespace/name "testns"})
    (println "Deleted namespace")
    (da/delete-tenant! {:pulsar/admin-client c
                        :pulsar.tenant/name "test-tenant"})))

