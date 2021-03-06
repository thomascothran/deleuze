(ns pro.thomascothran.deleuze.alpha.serializers
  (:require [abracad.avro.edn :as aedn]
            [abracad.avro :as avro])
  (:import [java.util UUID]))

(extend-protocol aedn/EDNAvroSerializable
  UUID
  (-schema-name [_] "pro.thomascothran.deleuze.alpha.serializers.UUID") ;; name as something else?
  (field-get [this field]
    (def this this)
    (def field field)
    this)
  (field-list [this] #{:value}))
(extend-protocol avro/AvroSerializable
  UUID
  (schema-name [_] "string")
  (field-get [this _field]
    (str this))
  (field-list [this]
    [:value]))

(defn ->uuid
  [s]
  (UUID/fromString s))

(def -additional-elements
  [{:type "record" :name "pro.thomascothran.deleuze.alpha.serializers.UUID"
    :fields [{:name "value" :type "string"}]}])

(def -edn-in-avro-schema
  (aedn/new-schema -additional-elements))

(comment
  (->> {:abc 123 :utc (java.util.UUID/randomUUID)}
       (avro/binary-encoded -edn-in-avro-schema)
       (avro/decode -edn-in-avro-schema)))

(defn serialize
  ([x] (serialize :edn-in-avro x))
  ([schema-type x]
    (case schema-type
      :edn-in-avro
      (avro/binary-encoded -edn-in-avro-schema x))))

(defn deserialize
  ([x] (deserialize :edn-in-avro x))
  ([schema-type x]
   (case schema-type
     :edn-in-avro
     (binding [avro/*avro-readers*
               (assoc avro/*avro-readers*
                      'pro.thomascothran.deleuze.alpha.serializers/UUID
                      #'->uuid)]
       (avro/decode -edn-in-avro-schema x)))))

(comment
  (->> {:abc 123 :utc (java.util.UUID/randomUUID)}
       (serialize)
       (deserialize)))
