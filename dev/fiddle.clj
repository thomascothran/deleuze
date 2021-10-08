(ns fiddle
  (:require [portal.api :as p]))

;; Portal
(comment ; Open a new inspector
  (do (p/open) 
      (add-tap #'p/submit)))
(comment
  (p/clear))
(comment
  (do (remove-tap #'p/submit)
      (p/close)))

