(ns pro.thomascothran.deleuze.alpha.curator
  (:import [org.apache.curator.framework
            CuratorFrameworkFactory CuratorFramework]
           [org.apache.curator.retry ExponentialBackoffRetry]))

(defn client
  "Create and start a curator framework client"
  ^CuratorFramework
  [{:keys [:zookeeper/connection-string
           ::base-sleep-ms
           ::max-retries]
    :or {base-sleep-ms 1000
         max-retries 6}}]
  (let [retry (ExponentialBackoffRetry. base-sleep-ms max-retries)
        client (CuratorFrameworkFactory/newClient connection-string
                                                  retry)]
    (.start client)
    client))


(comment
  (let [zk-string "localhost:2181,localhost:2182"
        c (client {:zookeeper/connection-string zk-string
                   ::max-retries 2})]
    (def c c)))
