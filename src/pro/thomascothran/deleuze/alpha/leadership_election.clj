(ns pro.thomascothran.deleuze.alpha.leadership-election
  (:require [org.apache.curator.framework.recipes.leader
             LeaderLatch LeaderLatchListener]))

(defn latch
  "Start a leadership latch.

  Params
  ------
  - `:curator-framework/client`:
  - `:curator.leadership/path`: the path for the latch.
  - `:curator.leadership/on-leadership (optional) - a thunk called
     when leaderships is acquired
  - `:curator.leadership/on-lost-leadership` (optional) - a thunk
     called when leadership is lost.

  Returns a map in an atom:
  -----------
  - `:curator.latch/is-leader` (boolean).
  - `:curator.leadership/latch` (LeadershipLatch).
  - `:curator.leadership/close!` (thunk) closes the latch."
  [{c :curator-framework/client
    latch-path :curator.leadership/path
    on-leadership :curator.leadership/on-leadership
    on-lost-leadership :curator.leadership/on-lost-leadership}]
  (let [latch (LeaderLatch. c latch-path)
        a (atom {:curator.leadership/latch latch
                 :curator.leadership/close! #(.close latch)})
        listener
        (reify LeaderSelectorListener
          (isLeader []
            (swap! a assoc :curator.latch/is-leader true)
            (when on-leadership (on-leadership)))
          (notLeader []
            (swap! a assoc :curator.latch/is-leader false)
            (when on-lost-leadership (on-lost-leadership))))]
    (.addListener latch listener)
    (.start latch)
    a))
