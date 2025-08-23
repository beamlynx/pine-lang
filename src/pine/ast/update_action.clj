(ns pine.ast.update-action)

(defn handle [state assignments]
  (assoc state :update assignments))