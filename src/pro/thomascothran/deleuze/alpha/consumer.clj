(ns pro.thomascothran.deleuze.alpha.consumer
  (:require [pro.thomascothran.deleuze.alpha.topics
             :as topics]
            [clojure.walk :refer [keywordize-keys]]
            [taoensso.nippy :refer [thaw]])
  (:import [org.apache.pulsar.client.api SubscriptionType
            SubscriptionInitialPosition]))

(defn consumer
  [{:keys [:pulsar/client]
    subs-name :pulsar.subscription/name
    subs-type :pulsar.subscription/type
    tenant     :pulsar.tenant/name
    namespace  :pulsar.namespace/name
    topic      :pulsar.topic/topic
    #_#_schema     :pulsar.topic/schema ;; malli schema
    initial-position ::initial-position
    :or {initial-position ::earliest}
    :as opts}]
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
        initial-position' (case initial-position
                           ::earliest
                           (SubscriptionInitialPosition/Earliest)
                           ::latest
                           (SubscriptionInitialPosition/Earliest))
        topics [(topics/topic-str opts)]
        consumer' (doto (.newConsumer client)
                    (.topics topics)
                    (.subscriptionName subs-name)
                    (.subscriptionType subs-type')
                    (.subscriptionInitialPosition initial-position'))]
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
    (let [
          properties (-> (into {} (.getProperties msg))
                         keywordize-keys
                         (update :deleuze/serializer keyword))
          body (case (:deleuze/serializer properties)
                 :nippy/freeze
                 (-> (.getData msg) thaw))
          m {:pulsar.message/body body
             :pulsar.message/key (.getKey msg)
             :pulsar.message/properties properties
             :pulsar.topic/topic (.getTopicName msg)
             :pulsar.message/sequence-id (.getSequenceId msg)
             :pulsar.message/acknowledge!
             #(.acknowledge consumer msg)}]
      (when-not (= ::closed (callback m))
        (recur (.receive consumer))))))

