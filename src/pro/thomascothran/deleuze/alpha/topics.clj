(ns dev.thomascothran.deleuze.alpha.topics)

(defn topic-str
  [{persistent :pulsar.topic/persistent
    tenant     :pulsar.tenant/name
    namespace  :pulsar.namespace/name
    topic      :pulsar.topic/topic
    :or {persistent true}}]
  (assert tenant)
  (assert namespace)
  (assert topic)
  (str (if persistent
         "persistent" "non-persistent")
       "://" tenant "/"
       namespace "/" topic))

(defn all-topics
  [{client   :pulsar/admin-client
    tenant   :pulsar.tenant/name
    ns-name  :pulsar.namespace/name
    :as _opts}]
  (assert client)
  (assert tenant)
  (assert ns-name)
  (-> (.topics client)
      (.getList (str tenant "/" ns-name))))

(defn delete-topic!
 [{client :pulsar/admin-client
   _persistent :pulsar.topic/persistent
   _tenant     :pulsar.tenant/name
   _ns-name   :pulsar.namespace/name
   _topic      :pulsar.topic/topic
   :as opts}]
  (let [topic-str' (topic-str opts)]
    (-> (.topics client)
        (.delete topic-str'))))
