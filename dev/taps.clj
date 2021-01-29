(ns taps)

(defn start-reveal!
  []
  (add-tap ((requiring-resolve 'vlaaad.reveal.ui/make))))
(comment (start-reveal!))
