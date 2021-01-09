(ns dev.thomascothran.deleuze.alpha.admin
  (:import [org.apache.pulsar.client.admin PulsarAdmin]
           [org.apache.pulsar.common.policies.data TenantInfo]
           [java.util Set]))

(declare get-namespaces)

(defn client
  [{:keys [:pulsar.service/admin-url] :as _pulsar}]
  (assert admin-url)
  (-> (PulsarAdmin/builder)
      (.serviceHttpUrl admin-url)
      (.build)))

;; =========+
;; Clusters |
;; =========+
(defn clusters
  [{:keys [:pulsar/admin-client]}]
  (.clusters admin-client))
(comment
  (let [{:keys [pulsar]} (user/sys)]
    (clusters pulsar)))

;; ========+
;; Tenants |
;; ========+
(defn tenants
  [{:keys [:pulsar/admin-client]}]
  (-> (.tenants admin-client)
      (.getTenants)))
(comment
  (let [{:keys [pulsar]} (user/sys)]
    (tenants pulsar)))

(defn tenant-info
  [{admin-client :pulsar/admin-client
    tenant-name :pulsar.tenant/name}]
  (let [ti (-> (.tenants admin-client)
               (.getTenantInfo tenant-name))]
    {:pulsar.tenant/admin-roles (.getAdminRoles ti)
     :pulsar.tenant/allowed-clusters (.getAllowedClusters ti)}))

(defn create-tenant!
  [{:keys [:pulsar/admin-client]
    tenant-name :pulsar.tenant/name}]
  (-> (.tenants admin-client)
      (.createTenant tenant-name
                     (TenantInfo. #{} #{"standalone"}))))
(defn delete-tenant!
  [{admin-client :pulsar/admin-client
    tenant-name :pulsar.tenant/name}]
  (-> (.tenants admin-client)
      (.deleteTenant tenant-name)))

;; ===========+
;; Namespaces |
;; ===========+
(defn get-namespaces
  [{:keys [:pulsar/admin-client]
    tenant-name :pulsar.tenant/name}]
  (-> (.namespaces admin-client)
      (.getNamespaces tenant-name)))

(defn create-namespace!
  [{:keys [:pulsar/admin-client]
    tenant-name :pulsar.tenant/name
    nsn         :pulsar.namespace/name}]
  (assert nsn)
  (assert admin-client)
  (assert tenant-name)
  (let [full-nsn (str tenant-name "/" nsn )]
    (-> (.namespaces admin-client)
       (.createNamespace full-nsn))))

(defn delete-namespace!
  [{:keys [:pulsar/admin-client]
    tenant-name :pulsar.tenant/name
    nsn         :pulsar.namespace/name}]
  (assert nsn)
  (assert admin-client)
  (assert tenant-name)
  (let [full-nsn (str tenant-name "/" nsn)]
    (-> (.namespaces admin-client)
        (.deleteNamespace full-nsn))))

(defn namespace-policies
  [{:keys [:pulsar/admin-client]
    tenant-name :pulsar.tenant/name
    nsn         :pulsar.namespace/name}]
  (-> (.namespaces admin-client)
      (.getPolicies (str tenant-name "/" nsn))))
(comment
  (with-open [c (client {:pulsar.service/admin-url
                         "http://localhost:8080"})]
    (-> (namespace-policies
          {:pulsar/admin-client c
           :pulsar.namespace/name "testns"
           :pulsar.tenant/name "test-tenant"})
        (clojure.reflect/reflect)
        :members
        (clojure.pprint/print-table))))

(defn namespace-auth
  [{_admin-client :pulsar/admin-client
    _tenant-name :pulsar.tenant/name
    _nsn         :pulsar.namespace/name
    :as opts}]
  (-> (namespace-policies opts)
      (.auth_policies)
      (.namespace_auth)))

(comment
  (with-open [c (client {:pulsar.service/admin-url
                         "http://localhost:8080"})]
    (-> (namespace-auth
         {:pulsar/admin-client c
          :pulsar.namespace/name "testns"
          :pulsar.tenant/name "test-tenant"}))))
