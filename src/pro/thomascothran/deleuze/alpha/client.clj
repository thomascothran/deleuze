(ns pro.thomascothran.deleuze.alpha.client
  (:import [org.apache.pulsar.client.api PulsarClient]))


(defn topic-str
  [{persistent :pulsar.topic/persistent
    tenant     :pulsar.topic/tenant
    namespace  :pulsar.topic/namespace
    topic      :pulsar.topic/topic}]
  (str (if persistent
         "persistent" "non-persistent")
       "://" tenant "/"
       namespace "/" topic))

;; Topics

(defn client
  [{:keys [:pulsar.service/url]}]
  (-> (PulsarClient/builder)
    (.serviceUrl url)
    (.build)))

(comment
  (let [{:keys [pulsar]} (user/sys)]
    (client pulsar)))


(defn producer
  [{:keys [:pulsar/client] :as _pulsar
    persistent :pulsar.topic/persistent
    tenant     :pulsar.tenant/name
    namespace  :pulsar.namespace/name
    topic      :pulsar.topic/topic
    ;; schema     :pulsar.topic/schema ;; malli schema
    :or {persistent true}}]
  (assert namespace)
  (assert topic)
  (assert client)
  (assert tenant)
  (let [topic-str' #_topic
        (topic-str #:pulsar.topic
                              {:persistent persistent
                               :tenant     tenant
                               :namespace  namespace
                               :topic      topic})
        #_#_schema' (json-schema/transform schema)]
    (-> (.newProducer client)
        (.topic topic-str')
        (.create))))
(comment
  (let [client (client {:pulsar.service/url "pulsar://localhost:6650"})
        p (producer {:pulsar.namespace/name "testns"
                     :pulsar/client  client
                     :pulsar.tenant/name "test-tenant"
                     :pulsar.topic/topic "test-topic"
                     #_#_:pulsar.topic/schema [:map
                                                  [:msg string?]]})]
    p))

