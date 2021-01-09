(ns dev.thomascothran.deleuze.alpha.consumer
  (:require [dev.thomascothran.deleuze.alpha.topics
             :as topics])
  (:import [org.apache.pulsar.client.api SubscriptionType]))

(defn consumer
  [{:keys [:pulsar/client]
    subs-name :pulsar.subscription/name
    subs-type :pulsar.subscription/type
    tenant     :pulsar.tenant/name
    namespace  :pulsar.namespace/name
    topic      :pulsar.topic/topic
    #_#_schema     :pulsar.topic/schema ;; malli schema
    :as opts
    }]
  (assert subs-name)
  (assert subs-type)
  (assert tenant)
  (assert namespace)
  (assert client)
  (assert topic)
  (let [subs-type' (case subs-type
                     :shared SubscriptionType/Shared
                     :exclusive SubscriptionType/Exclusive
                     :failover SubscriptionType/Failover
                     :key-shared SubscriptionType/Key_Shared)
        topics [(topics/topic-str opts)]
        consumer' (doto (.newConsumer client)
                    (.topics topics)
                    (.subscriptionName subs-name)
                    (.subscriptionType subs-type'))]
    (.subscribe consumer')))

(defn receive-sync!
  "Params
  -------
  - `::callback` - a function that takes the pulsar message.
    If it returns ::closed, the loop will stop, otherwise it
    will continue."
  [{consumer :pulsar/consumer
    callback ::callback}]
  (loop [msg (.receive consumer)]
    (when-not (= ::closed (callback msg))
      (recur (.receive consumer)))))
