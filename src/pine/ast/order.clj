(ns pine.ast.order
  (:require [pine.ast.path :as path]))

(defn handle [state value]
  (let [i       (state :index)
        columns (map #(-> (path/resolve-column state %1)
                          (assoc :operation-index i))
                     value)]
    (-> state
        (update :order into columns))))
