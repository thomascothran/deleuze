(ns pro.thomascothran.deleuze.alpha.locks
  (:require [next.jdbc :as jdbc]))

(defn ->lock-code
  "Takes a keyword and converts it to a bigint."
  [x]
  (-> (.hashCode x)
      (BigInteger/valueOf)))
(comment
  (->lock-code :a))

(defn lock!
  "Acquires a lock that will only be released when
  `unlock!` is called or the session closes.

  Blocks until a lock is acquired.

  Parameters
  ===========
  - `conn`: the database connection for postgres.
  - `lock-name`: a string or keyword for the name of the
     lock.
  - `on-lock!`: a thunk called when the lock is acquired. "
  [{:keys [conn lock-name on-lock!]
    :as opts}]
  (assert lock-name)
  (jdbc/execute! conn
                 [(str "SELECT "
                       "pg_advisory_lock"
                       "(CAST (? as BIGINT))")
                  (->lock-code lock-name)])
  (on-lock!)
  opts)

(defn unlock!
  "Parameters
  ===========
  - `conn`: the database connection for postgres.
  - `lock-name`: a string or keyword for the name of the
     lock."
  [{:keys [conn lock-name]}]
  (jdbc/execute! conn
                 [(str "SELECT "
                       "pg_try_advisory_unlock"
                       "(" (.hashCode lock-name) ")")]))
