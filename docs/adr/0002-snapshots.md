# Recomputing Snapshots

- Status: accepted
- Date created: 2021-05-22


## Context

In an event sourced application, the current state of an aggregate is a reduction over the events for that aggregate. However, this can be slow for aggregates comprising many events. As a result, we keep snapshots of the aggregate that update each time an event is emitted for an aggregate.

## Problem statement

If we update the reducing function, the snapshots are no longer accurate. We need a way of both keeping snapshots (for the normal case) and recomputing snapshots when our reducing function needs to change.

## Options

### Run a batch job that updates the snapshots.

This could be a script that runs a for loop over all the aggregates of a certain type, and updates the snapshot. This has the upside of being simple to write. However, the downsides are significant:

- Some snapshots will be stale and some won't until the script completes.
- If the script dies, we won't know which snapshots have been updated and which have not.

### The `current-state` recomputes the state when there is no snapshot, and updates the snapshot

The `current-state` could do the following:

```
IF there is a snapshot, use it.
IF there is no shapshot:
  Recompute the current state
  Save it to the snapshots table
  Return the snapshot
```
This way we could simply drop any stale snapshots, and the system would recompute them as needed. The down side is that if there are many calls to `current-state` for many aggregates, the system will slow down.

## Decision

Change `current-state` to recompute and save the current state if it is not in the table.
