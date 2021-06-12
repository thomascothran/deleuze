# Deleuze

Deleuze is a collection of tools for working with Apache Pulsar, Postgres, and Clojure. The design goals are fairly limited, and it is not intended to be a general purpose library. 

## Design Goals and Non-Goals

Deleuze is somewhat bespoke project designed to:

1. Reduce repitition across projects and lower cognitive burden by using a common set of tools
2. Provide inter operability with other systems via Apache Pulsar
3. Reify changes to business systems

## Getting started

Apache Pulsar - need to set up Debezium and Tiered Storage.

## Core Problems

Deleuze is used with three distinct sets of problems:

### Event Sourcing

The `pro.thomascothran.deleuze.alpha.event-sourcing` namespace provides a set of tools for event sourcing. Events are stored in Postgres and propagated through Debezium.

### Event Streaming

A more loosely connected set of tools for event streaming. 

### Ad Hoc Distributed Computing Tools
