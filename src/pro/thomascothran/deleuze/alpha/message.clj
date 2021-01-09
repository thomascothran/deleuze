(ns dev.thomascothran.deleuze.alpha.message
  (:import [org.apache.pulsar.client.api Message]
           [java.time Instant]))

(defn send!
  [{producer :pulsar/producer
    k :pulsar.message/key
    v :pulsar.message/value
    properties :pulsar.message/properties
    :or {properties {}}}]
  (assert producer)
  (assert k)
  (assert v)
  (let [new-message (doto (.newMessage producer)
                      (.key (str k))
                      (.value (.getBytes v))
                      (.properties properties))]
    (.send new-message)))
