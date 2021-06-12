# Propagating Events with Event Sourcing

* Status: investigation
* Date created: 2021-05-22

## Context

Question for Pulsar slack.

I'm evaluating Pulsar for a CQRS/event sourced application. In an event source application, the state is a reduction over a series of events that together constitute an aggregate. 

An event store table would be created like this:

```sql
CREATE TABLE event_store ( 
    aggregate_id   UUID NOT NULL,
    version        BIGINT NOT NULL,
    occurred_at    TIMESTAMP NOT NULL,
    created_at     TIMESTAMPTZ default now(),
    event          BYTEA NOT NULL,
    PRIMARY KEY   (aggregate_id, version)
)
```

This has the important property that you cannot have two events with the same aggregate id and version. This makes it possible to guarantee consistency when updating an aggregate. Methods insert events in the `event_store` table to update the state of the aggregate. By checking what version the current state of the table is, the method won't update the state if there's a new event it hasn't seen yet.


### Example Problems

#### Simple Example

Suppose we have an event source `:bank-account` entity. Possible events include `:open-account`, `:deposit`, `:withdraw`, and `:close-account`.

Consider the following sequence:

| Time | Event                                                    | Balance | Version |
|------|----------------------------------------------------------|---------|---------|
| T1   | `:open-account` event occurs                             |       0 | 0       |
| T2   | `:deposit` $50 command occurs                            |       0 | 0       |
| T3   | server 1 command handler emits `:deposit` $50 event      |      50 | 1       |
| T4   | server 1 `:withdraw` $50 command handler called          |      50 | 1       |
| T5   | server 2 `:withdraw` $50 command handler called          |      50 | 1       |
| T6   | server 1 `:withdraw` $50 command handler emits its event |       0 | 2       |
| T7   | server 2 tries to emit `:withdraw` $50 event             |       ? | ?       |

At T6, the current state has updated so that the balance in the account is now $0. However, server 2's command handler has the current state at version 1, and so it thinks there's still an account balance of $50.

Using traditional event-sourcing/CQRS patterns, the event that is emitted by server 2 at T7 will have a version property set to 1. This violates the constraint of the SQL database, and so it fails, throwing an error.

## Problem Statement

The `pro.thomascothran.deleuze.alpha.event-sourcing` namespace provides an event sourcing framework. The `log-event!` function saves events in a Postgres table and propagates them using Pulsar.

Currently, however, an event may sent to the message broker before the transaction succeeds. The implementation looks like this:

```
(jdbc/with-transaction [tx datasource]
  (sql/insert! tx :deleuze_event_store row)         ;; 1. insert the event
  (-update-state! (assoc opts :datasource tx))      ;; 2. update the snapshot
  (with-open [producer (dc/producer producer-opts)]
    (dp/send! {:pulsar/producer producer            ;; 3. Send the event to
               :pulsar.message/value event           ;; the message broder
               :pulsar.message/key agg-id})))        ;; 4. Commit db changes
```

It is possible for an event to be propagated via the message broker but not stored in the database. The database serves as the official event store. The client sending the event thinks the event has been stored, the downstream consumer think the event has occurred, but the single source of truth for the aggregate state does not know about the event.

## Options

### 1. Use the outbox pattern.

In a single transaction, write the event to the `deleuze_event_store` table and the event id to a `deleuze_event_queu` table. Poll the table for new events, send them to Pulsar, and remove them from the table.

But this introduces several complexities:

- In a cluster setup, we would not want all instances listening to the table and attempting to load the events into Pulsar. [Message de-duplication](https://pulsar.apache.org/docs/en/cookbooks-deduplication/) helps here, but it would still be messy.
- This introduces a delay between when events are put into the table and when they are read from the table. Postgres supports `NOTIFY`, but the standard JDBC driver uses polling. However, the [gjdbc-ng](http://impossibl.github.io/pgjdbc-ng/) driver does support asynchronous notifications.

### 2. Entirely separate event-sourcing from event propagation, using something like Debezium.

We could completely separate the event propagation from the event sourcing logic. Applications that use the library could use Debezium to load events from the event table into Pulsar (or Redis or Kafka or whatever they choose).

### 3. Use Pulsar as is

Don't build anything special. We can handle consistency on an as-needed basis. 

For example, in the *Simple Example* above, we could have the followings streams:

- `account_events` - all events are on this stream.

The `account_events` stream receives events in the following order:

```clojure
[{:type :open-account}
 {:type :deposit, :amt 50.0}
 {:type :withdrawal-request, :amt 50.0}
 {:type :withdrawal-request, :amt 50.0}]
```

A single thread listens to the `account_events` stream. But it must track the current state. It can do this on an as-needed basis, with a SQL database, using Pulsar SQL, or even building up state in memory if necessary.

The `event-listener` function will query for the current state. If there are adequate funds in the account, it will put this event in the `account_events`:

```clojure
{:type :withdrawal-succeeded :amt 50.0}
```

Else

```clojure
{:type :withdrawal-failed :amt 50.0}
```

So at the end, the `account_events` stream would look like:

```clojure
[{:type :open-account}
 {:type :deposit, :amt 50.0}
 {:type :withdrawal-request, :amt 50.0}
 {:type :withdrawal-request, :amt 50.0}
 {:type :withdrawal-succeeded :amt 50.0}
 {:type :withdrawal-failed :amt 50.0}]
```

The reducer would look something like:

```clojure
(defn event-handler
  "Returns any further events to be added to
  the `account_events` stream."
  [{:keys [amount] :as current-state}
   {event-type :type :as event}]
    (case event-type
      :withdrawal-request
      (let [requested-amt (:amt event)]
        (if (< amount requested-amt)
          {:type :withdrawal-failed
           :amt requested-amt}
          {:type :withdrawal-succeeded
           :amt requested-mat}))
      :else nil))
```

### 4. Put all events and commands into a single stream, and detect version changes

We could put all events and commands into a single stream, and have a single threaded handler reject the ones that have conflicting version numbers. In this case, the sole thing we need to know is what the last version is for an aggregate. This is much easier to put into memory, or a local store.

For example, we would have something like the following streams:

- `submitted_events` - all events go into this stream. They use the key of the aggregate id to which they are relevant.
- `rejected_events` - events which had concurrency conflicts go here.
- `events` - accepted events are put here.

All commands and events go into the `submitted_events` stream. They have the following properties:

- `version` - if supplied, the previous version for an event
- `id` - UUID for the event
- `data` - the command data.

We then have function listening on a single thread, which does the following:

```
IF event.version AND event.prevVersion != aggregate.currentVersion + 1
  THEN emit event TO `rejected_events`
ELSE
  emit event to `events`
```

However, we have a few problems to solve here. 

*Current version* What is the best way to get the current version of an aggregate? This is trickier than it looks. We can't query the last event in the `submitted_events` stream. We could query the last event version for that aggregate in the `events** stream, but we would have to make sure that it has caught up. If we have another process listening to that stream and then storing the event version, that is only eventually consistent, and we may not have the most recent event. Finally, we could synchronously update a database, but this brings back in the database and may not be easier to work with.

*Current State* With a database, it is easy to get all the events for an aggregate. But with Pulsar, we would likely either have to replay the whole sequence of every aggregate of that type, or else use Pulsar SQL (which would have to be set up and configured).

## Decision

We will use option #2. Although it imposes some extra setup, it is likely the easiest and the most decoupled.
