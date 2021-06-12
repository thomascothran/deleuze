(ns pro.thomascothran.deleuze.alpha.pulsar-utils
  "Utilities for setting up and testing pulsar."
  (:require [pro.thomascothran.deleuze.alpha.admin :as da]
             [malli.core :as mi]
             [malli.error :as me]
            [pro.thomascothran.deleuze.alpha.topics :as dt])
  (:import [org.apache.pulsar.client.admin PulsarAdmin]))

(def SetupPulsarOpts
  [:map
   [:pulsar/admin-client
    [:fn (fn [x] (instance? PulsarAdmin x))]]
   [:pulsar.tenant/name string?]
   [:pulsar.namespace/name {:optional true}
    string?]])
(defn setup-pulsar!
  "Idempotent function to setup pulsar, e.g., for tests.

  Sets up the tenant and the namespace.

  Params:
  ------
  - `:pulsar.tenant/name`: the name of the application
  - `:pulsar.namespace/name` - the namespace you want to create.
  - `:pulsar/admin-client`: the pulsar admin client"
  [{tenant-name   :pulsar.tenant/name
    namespace-name :pulsar.namespace/name
    :as opts}]
  (when-not (mi/validate SetupPulsarOpts opts)
    (let [err (mi/explain SetupPulsarOpts opts)]
      (throw (ex-info "Invalid opts"
                      {:error err
                       :type ::invalid-setup-pulsar-opts
                       :msg (me/humanize err)}))))
  (let [opts' (->  opts
                   (assoc :pulsar.namespace/name
                          namespace-name))
        tenants
        (->> (da/tenants opts') set)
        tenant-exists? (tenants tenant-name)
        namespaces
        (when tenant-exists?
          (->> (da/get-namespaces opts') set))
        full-ns (str tenant-name "/" namespace-name)
        namespace-exists?
        (when tenant-exists?
          (namespaces full-ns))]
    (when-not tenant-exists?
      (da/create-tenant! opts'))
    (when (and (not namespace-exists?) namespace-name)
      (da/create-namespace! opts'))))
(comment
  (with-open [ac (da/client {:pulsar.service/admin-url
                             "http://localhost:8080"})]
    (setup-pulsar! {:pulsar.tenant/name "abc"
                    :pulsar/admin-client ac})))

(defn teardown-pulsar!
  "Removes pulsar setup. For use in, e.g., testing."
  [{admin-client :pulsar/admin-client
    tenant-name  :pulsar.tenant/name
    namespace-name :pulsar.namespace/name
    :as opts}]
  (assert admin-client)
  (assert tenant-name)
  (let [opts' (assoc opts :pulsar.namespace/name
                     namespace-name
                     ::dt/force-delete true)]
    (dt/delete-all-topics! opts')
    (when namespace-name
      (da/delete-namespace! opts'))
    (da/delete-tenant! opts')))
(comment
  (with-open [ac (da/client {:pulsar.service/admin-url
                             "http://localhost:8080"})]
    (teardown-pulsar! {:pulsar.tenant/name "abc"
                        :pulsar/admin-client ac})))

