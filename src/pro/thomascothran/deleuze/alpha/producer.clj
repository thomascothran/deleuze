(ns pro.thomascothran.deleuze.alpha.producer
  (:require [taoensso.nippy :refer [freeze]]
            [clojure.string])
  #_(:import [org.apache.pulsar.client.api SubscriptionType]))

(defn send!
  [{producer :pulsar/producer
    k :pulsar.message/key
    v :pulsar.message/value
    properties :pulsar.message/properties
    serializer :pulsar/serializer
    :or {serializer :nippy/freeze
         properties {}}}]
  (assert producer)
  (assert k)
  (assert v)
  (let [properties' (assoc properties "deleuze/serializer"
                           (-> (str serializer)
                               (subs 1)))
        serializer (case serializer
                     :nippy/freeze freeze)
        new-message (doto (.newMessage producer)
                      (.key (str k))
                      (.value (serializer v))
                      (.properties properties'))]
    (.send new-message)))
