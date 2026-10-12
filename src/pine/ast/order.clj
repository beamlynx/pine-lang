(ns pine.ast.order
  (:require [pine.ast.path :as path]))

(defn handle [state value]
  (let [i       (state :index)
        ;; A computed column's name sorts by its value (path/computed-ref).
        columns (map #(-> (or (path/computed-ref state %1) (path/resolve-column state %1))
                          (assoc :operation-index i))
                     value)]
    (-> state
        (update :order into columns))))
