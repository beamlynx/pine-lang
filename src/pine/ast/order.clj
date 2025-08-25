(ns pine.ast.order)

(defn handle [state value]
  (let [i       (state :index)
        current (state :current)
        columns (map #(-> %1
                          (assoc :alias (or (:alias %1) current))
                          (assoc :operation-index i))
                     value)]
    (-> state
        (update :order into columns))))
