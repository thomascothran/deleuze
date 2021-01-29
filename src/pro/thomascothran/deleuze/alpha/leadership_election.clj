(ns pro.thomascothran.deleuze.alpha.leadership-election
  (:import [org.apache.curator.framework.recipes.leader
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
    latch-id :curator.participant/id
    latch-path :curator.leadership/path
    on-leadership :curator.leadership/on-leadership
    on-lost-leadership :curator.leadership/on-lost-leadership}]
  (assert c)
  (assert latch-path)
  (assert latch-id)
  (let [latch (LeaderLatch. c latch-path latch-id)
        a (atom {:curator.participant/id latch-id})
        listener
        (reify LeaderLatchListener
          (isLeader [this]
            (swap! a assoc :curator.latch/is-leader true)
            (when on-leadership (on-leadership)))
          (notLeader [this]
            (swap! a assoc :curator.latch/is-leader false)
            (when on-lost-leadership (on-lost-leadership))))]
    (swap! a assoc
           :curator.leadership/latch latch
           :curator.latch/close! #(do (.removeListener latch listener)
                                      (.close latch)))
    (.addListener latch listener)
    (.start latch)
    a))

(defn -inspect
  [latch]
  {:curator.latch/state (.getState latch)
   :curator.latch/participant-id (.getId latch)
   :curator.latch/leader-id (-> (.getLeader latch)
                                (.getId))
   :curator.latch/partipants (.getParticipants latch)
   :curator.latch/our-path (.getOurPath latch)})

(defn inspect
  [latch-atm]
  (-> @latch-atm :curator.leadership/latch -inspect))

(defn -await-leader
  ([latch] (-await-leader latch
                          {::max-millis 120000
                           ::retry-strategy ::inc-10sec}))
  ([latch {:keys [::max-millis ::retry-strategy]
           :as opts}]
   (loop [millis 0]
     (let [has-leader (-> (.getLeader latch) (.isLeader))
           inc-fn (case retry-strategy
                    ::inc-10sec #(+ % 10000))
           next-millis (inc-fn millis)]
      (cond has-leader
          (do (println (assoc opts :msg "Has a leader"))
              latch)
          (> next-millis max-millis)
          (throw (ex-info "Reached timeout"
                          (assoc opts
                                :type ::await-leader-timeout
                                :next-millies next-millis)))
          :else (do (Thread/sleep (- next-millis millis))
                    (recur next-millis)))))))

(defn await-leader
  "Block until leader is known to be acquired."
  [latch-atm]
  (-await-leader (-> @latch-atm :curator.leadership/latch)))
