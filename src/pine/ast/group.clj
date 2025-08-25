(ns pine.ast.group
  (:require
   [clojure.string :as s]))

(defn handle [state value]
  (let [i (state :index)
        current (state :current)
        columns (map #(-> %1
                          (assoc :alias (or (:alias %1) current))
                          (assoc :operation-index i))
                     (:columns value))
        fn-columns (map (fn [name] {:symbol (str (s/upper-case name) "(1)")}) (:functions value))]
    (-> state
        (assoc :columns (into fn-columns columns)) ;; set the columns to be aggregated
        (update :group into columns))))

