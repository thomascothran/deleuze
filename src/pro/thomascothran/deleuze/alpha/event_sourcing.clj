(ns pro.thomascothran.deleuze.alpha.event-sourcing
  "An opinionated event sourcing framework.

  Core decisions:
  - Commands are synchronous, leveraging postgres'
    constraints (on aggregate id and version).
  - Single source of truth for events is Postgres
    + Pulsar used for replays.
    + Postgres used for computing current state.
  - Message propagation is a separate concern.
    + Recommend using Debezium
  - Snapshots are used for the current state.

  TODO
  - Provide a mechanism to recompute current state.
    + Maybe load into a pulsar topic and delete when done?
  "
  (:require [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]
            [next.jdbc.date-time]
            [taoensso.nippy :refer [freeze thaw]]
            [clojure.set :refer [rename-keys]]
            [malli.core :as mi]
            [malli.error :as me]
            [malli.util :as mu]
            #_[java.sql Connection])
  (:import [javax.sql DataSource]))


(defn kw->str
  [kw]
  (cond (string? kw)
        kw
        (keyword? kw)
        (-> kw str (subs 1))
        :else
        (throw (ex-info "Wrong type"
                        {:kw kw}))))
(comment
  (kw->str :a.b/c)
  (-> (kw->str :c.d/e)
      keyword))

(defn -create-tables!
  "Idempotent. Creates event table."
  [{:keys [:datasource]}]
  (let [event-store-table "deleuze_event_store"
        snapshot-table "deleuze_aggregate_snapshots"
        event-store
        [(str "CREATE TABLE IF NOT EXISTS "
              event-store-table " "
              "( "
              "  aggregate_id   UUID NOT NULL,"
              "  aggregate_name TEXT NOT NULL,"
              "  tenant_name    TEXT,"
              "  serializer     TEXT NOT NULL,"
              "  version        BIGINT NOT NULL,"
              "  occurred_at    TIMESTAMP NOT NULL,"
              "  created_at     TIMESTAMPTZ default now(),"
              "  meta           JSONB,"
              "  event          BYTEA NOT NULL,"
              "  PRIMARY KEY   (aggregate_id, version)"
              ")")]
        event-snapshots
        [(str "CREATE TABLE IF NOT EXISTS "
              snapshot-table " "
              "( "
              "  aggregate_id   UUID NOT NULL,"
              "  tenant_name    TEXT,"
              "  namespace_name TEXT,"
              "  serializer     TEXT NOT NULL,"
              "  aggregate_name TEXT NOT NULL,"
              "  version        BIGINT NOT NULL,"
              "  created_at     TIMESTAMPTZ default now(),"
              "  state          BYTEA NOT NULL,"
              "  PRIMARY KEY    (aggregate_id, version)"
              ")")]]
    (jdbc/with-transaction [tx datasource]
      (jdbc/execute! tx event-store)
      (jdbc/execute! tx event-snapshots))))

(defn -delete-tables!
  [{:keys [:datasource]}]
  (jdbc/execute! datasource ["DROP TABLE deleuze_event_store"])
  (jdbc/execute! datasource ["DROP TABLE deleuze_aggregate_snapshots"]))


(defn setup!
  "Setup database and pulsar for deleuze event sourcing."
  [opts]
  (-create-tables! opts))

(defn teardown!
  "Deletes database tables and pulsar tenants/namespaces related.
  For testing purposes."
  [opts]
  (-delete-tables! opts))

(defn state
  "Returns the current state of an aggregate."
  [{agg-id      :aggregate/id
    agg-name    :aggregate/name
    tenant-name :pulsar.tenant/name
    datasource  :datasource}]
  (assert (uuid? agg-id))
  (assert (keyword? agg-name))
  (some-> (sql/find-by-keys datasource
                            :deleuze_aggregate_snapshots
                            {:aggregate_id agg-id
                             :tenant_name tenant-name
                             :aggregate_name (kw->str agg-name)})
          first
          (rename-keys {:deleuze_aggregate_snapshots/state
                        :aggregate/state
                        :deleuze_aggregate_snapshots/version
                        :aggregate/version})
          (update :aggregate/state #(some-> % thaw))
          (select-keys [:aggregate/state :aggregate/version])))

(def Event
  [:map
   [:event/id uuid?]
   [:event/type keyword?]
   [:aggregate/version number?]
   [:event/occurred-at inst?]
   [:aggregate/name keyword?]
   [:aggregate/id uuid?]])

(def Command
  [:map
   [:command/type keyword?]
   [:command/issued-at inst?]
   [:aggregate/name keyword?]
   [:aggregate/id uuid?]])

(def -Reducer
  any?
  #_[:=> [:tuple map? Event] map?])

(def UpdateStateArgs
  [:map
   [:event Event]
   [:reducer -Reducer]
   [:state-schema [:fn mi/schema?]]])
(defn -update-state!
  "Updates the state of the aggregate and stores it in the
  snapshots table."
  [{{version   :aggregate/version
     agg-id    :aggregate/id
     agg-name  :aggregate/name
     :as event} :event
    :keys [:datasource :reducer :state-schema]
    :as opts}]
  (assert agg-id)
  (assert agg-name)
  (assert version)
  (let [{prev-state :aggregate/state}
        (state {:datasource datasource
                :aggregate/id agg-id
                :aggregate/name agg-name})
        new-state (reducer prev-state event)]
    (when-not (mi/validate state-schema new-state)
      (let [err (mi/explain state-schema new-state)]
        (throw (ex-info "New state is invalid"
                        {:opts opts
                         :type ::invalid-new-state
                         :new-state new-state
                         :prev-state prev-state
                         :error-data err
                         :error-msg (me/humanize err)}))))
    (if (= 0 version)
      (sql/insert! datasource :deleuze_aggregate_snapshots
                   {"aggregate_id" agg-id
                    "aggregate_name" (kw->str agg-name)
                    "version" version
                    "state" (freeze new-state)})
      (sql/update! datasource :deleuze_aggregate_snapshots
                   {"state" (freeze new-state)
                    "version" (:aggregate/version event)}
                   {"aggregate_id" agg-id}))
    new-state))

(def EventsOpts
  [:map
   [:datasource [:fn (fn [ds] (instance? DataSource ds))]]
   [:aggregate/name keyword?]
   [:aggregate/id uuid?]])
(defn events
  [{aggregate-name :aggregate/name
    aggregate-id   :aggregate/id
    datasource     :datasource
    :as            opts}]
  (when-not (mi/validate EventsOpts opts)
    (throw (ex-info "Invalid opts"
                    (assoc opts
                           :type ::invalid-events-opts
                           :error-data
                           (mi/explain EventsOpts opts)))))
  (sql/find-by-keys datasource :deleuze_event_store
                    {"aggregate_name" aggregate-name
                     "aggregate_id"   aggregate-id}
                    {:order-by [:version]}))

(def CurrentVersionOpts
  [:map
   [:datasource [:fn (fn [ds] (instance? DataSource ds))]]
   [:aggregate/name keyword?]
   [:aggregate/id uuid?]])
(defn current-version
  [{aggregate-name :aggregate/name
    aggregate-id   :aggregate/id
    datasource     :datasource
    :as            opts}]
  (when-not (mi/validate CurrentVersionOpts opts)
    (throw (ex-info "Invalid opts"
                    (assoc opts
                           :type ::invalid-current-version-opts
                           :error (mi/explain CurrentVersionOpts)))))
  (-> (jdbc/execute-one! datasource
                         [(str "SELECT version FROM "
                               "deleuze_event_store "
                               "WHERE aggregate_name = ? "
                               "AND aggregate_id = ? "
                               "ORDER BY version DESC "
                               "LIMIT 1")
                          (kw->str aggregate-name) aggregate-id])
      (get :deleuze_event_store/version)))

(def UpdateOpts
  [:map
   [:events/schema
    [:fn mi/schema?]]
   [:event Event]
   [:reducer -Reducer]
   [:datasource
    [:fn (fn [ds] (instance? DataSource ds))]]])

(defn -update!
  "Update an aggregate with an event.

  Verifies that there's not a concurrency conflict,
  updates the state snapshot, and puts an event
  on the pulsar topic.

  Params
  ------
  - `:pulsar.tenant/name`: The name of the tenant. This is not the
    pulsar tenant (which should be the application name) but the
    tenants within the application itself.
  - `event`: the event.
  - `reducer`: a function that takes the current state of
    the aggregate and the event, and produces a new state.
  - `events/schema`: a malli schema for the events."
  [{event-schema :events/schema
    {version  :aggregate/version} :event
    serializer     :serializer
    :or {serializer :edn-in-avro}
    :keys [:datasource :event]
    :as opts}]
  (when-not (mi/validate UpdateOpts opts)
    (throw (ex-info "Invalid opts"
                    {:error-data (mi/explain UpdateOpts opts)
                     :err-msg (me/humanize (mi/explain UpdateOpts opts))
                     :opts opts
                     :type ::invalid-update-opts})))
  (when-not (mi/validate event-schema event)
    (let [err (mi/explain event-schema event)]
      (throw (ex-info "Invalid event"
                      {:error-data err
                       :opts opts
                       :type ::invalid-update-event
                       :error-msg (me/humanize err)}))))
  (let [serializer-fn (case serializer
                        :edn-in-avro
                        #(avro/binary-encoded @-edn-in-avro-schema %))
        row
        (-> (rename-keys event {:event/occurred-at :occurred_at
                                :aggregate/version :version
                                :pulsar.tenant/name       :tenant_name
                                :aggregate/name    :aggregate_name
                                :aggregate/id      :aggregate_id})
            (update :aggregate_name kw->str)
            (assoc :event (serializer-fn event))
            (select-keys [:version :aggregate_name :occurred_at
                          :aggregate_id :event :serializer]))
        current-version'
        (current-version (assoc event :datasource datasource))]
    (when (or (and (= 0 version) (not (nil? current-version')))
              (and (not= 0 version) (not= current-version' (dec version))))
      (throw (ex-info "Version mismatch"
                      {:current-version current-version'
                       :new-version version
                       :event event
                       :type ::version-mismatch})))
    (try (jdbc/with-transaction [tx datasource]
           (sql/insert! tx :deleuze_event_store row)
           (-update-state! (assoc opts :datasource tx)))
         (catch Exception e
           (throw (ex-info "Error updating aggregate"
                           (assoc opts
                                  :error e
                                  :type ::unknown-update-error)))))))

(def LogEventOpts
  [:map
   [:datasource [:fn (fn [ds] (instance? DataSource ds))]]
   [:event (mu/dissoc Event :aggregate/version)]])

(defn log-event!
  "Store an event.

  Whereas `fire-command!` will attempt to update the state and emit
  an event if successful, `log-event!` assumes the event has happened
  and will increment the version until successful."
  [{event      :event
    datasource :datasource
    max-attempts ::max-attempts
    :or {max-attempts 25}
    :as opts}]
  (when-not (mi/validate LogEventOpts opts)
    (let [err (mi/explain LogEventOpts opts)]
      (throw (throw (ex-info "Invalid opts"
                             {:type ::log-event-opts
                              :error err
                              :err-msg (me/humanize err)})))))
  (letfn [(update! [version]
            (try (-update! (assoc-in opts [:event :aggregate/version]
                                     version))
                 ::success
                 (catch Exception e
                   e)))]
    (loop [version (or (some-> (current-version
                               (assoc event :datasource datasource))
                              inc)
                      0)
          attempt 0]
      (let [result (update! version)]
        (cond (= ::success result)
              result
              (>= attempt max-attempts)
              (throw result)
              :else
              (recur (inc version) (inc attempt)))))))

(def FireCommandOpts
  [:map
   [:command Command]
   [:command/schema [:fn mi/schema?]]
   [:command/handler
    {:doc "A function that takes the previous state and returns an event"}
    [:fn fn?]]
   [:datasource [:fn (fn [ds] (instance? DataSource ds))]]
   [:reducer -Reducer]
   [:events/schema
    [:fn mi/schema?]]])

(defn fire-command!
  "Fire a command.

  Will fetch the current state, apply the `:command/handler` to
  the current state and the command, and then fire the event.
  If there is a concurrency conflict, retries until `::max-attempts`
  ceiling reached.

  Params:
  - `:datasource` - a `jdbc` datasource.
  - `:command` - the command to be attempted
  - `:command/schema` - the malli schema for the command.
    Typically a `:multi` schema keyed on `:command/type`.
  - `:reducer` - a function that takes the current state
    of the aggregate and the event, and produces the next
    state.
  - `:event/schema` - the malli schema for an event. Typically
    a `:multi` schema keyed on the event type.
  - `:command/handler`: takes the state and the command and
     returns an event, except for the `aggregate/version`, which
     is supplied automatically.
  - `::max-attempts`: the maximum number of retires in case of
    concurrency conflict.
  - `:tenant/name` - ??? pulsar tenant or application tenant?"
  [{command          :command
    command-handler  :command/handler
    command-schema   :command/schema
    datasource       :datasource
    max-attempts     ::max-attempts
    :or {max-attempts 1}
    :as opts}]
  (when-not (mi/validate FireCommandOpts opts)
    (let [err (mi/explain FireCommandOpts opts)]
      (throw (ex-info "Invalid opts"
                      {:type ::fire-command-opts
                       :error err
                       :error-msg (me/humanize err)}))))
  (when-not (mi/validate command-schema command)
    (let [err (mi/explain command-schema command)]
      (throw (ex-info "Invalid command"
                      {:type ::fire-command-command
                       :error err
                       :err-msg (me/humanize err)}))))
  (letfn [(update! [event]
            (try (-update! (assoc opts :event event))
                 ::success
                 (catch Exception e
                   e)))
          (known-error? [e]
            (let [known-errors #{::invalid-update-opts
                                 ::invalid-update-event}
                  err-type (-> (ex-data e) :type)]
              (known-errors err-type)))]
      (loop [attempt 1]
        (let [{prev-state   :aggregate/state
               prev-version :aggregate/version}
              (state (assoc command :datasource datasource))
              event (-> (command-handler prev-state command)
                        (assoc :aggregate/version (inc prev-version)))
              result (update! event)]
          (cond (= ::success result)
                ::success
                (>= attempt max-attempts)
                (throw result)
                (known-error? result)
                (throw result)
                :else
                (recur (inc attempt)))))))
