(ns pro.thomascothran.deleuze.alpha.schema
  (:require [malli.json-schema :as json-schema]
            [jsonista.core :as j])
  (:import [org.apache.pulsar.client.api.schema SchemaDefinition]
           [org.apache.pulsar.client.impl.schema
            AvroSchema
            JSONSchema
            SchemaDefinitionBuilderImpl]))

(set! *warn-on-reflection* true)

(defn ->json-schema
  [^SchemaDefinition schema-def]
  (JSONSchema/of schema-def))

(defn <-json-schema
  "WIP "
  [malli-schema]
  (let [json-def (-> (json-schema/transform malli-schema)
                     (j/write-value-as-string))
        _ (def json-def json-def)

        schema-def (-> (SchemaDefinitionBuilderImpl.)
                       (.withJsonDef json-def)
                       (.build))
        schema (->json-schema schema-def)]
    #_schema
    schema-def))
(comment
  (->> (<-json-schema [:map [:test-field string?]])
       #_(instance? SchemaDefinition)
       #_(.getJsonDef)))
