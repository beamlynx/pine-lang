(ns pine.ast.order)

(defn handle [state value]
  (let [i             (state :index)
        current       (state :current)
        ;; A live alias (e.g. re-bound via `as`) always wins over a stale |= snapshot
        resolve-alias #(if (contains? (:aliases state) %) % (or (get-in state [:pending-assignments % :current]) %))
        columns (map #(-> %1
                          (assoc :alias (resolve-alias (or (:alias %1) current)))
                          (assoc :operation-index i))
                     value)]
    (-> state
        (update :order into columns))))
