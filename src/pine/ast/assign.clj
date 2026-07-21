(ns pine.ast.assign
  "Records a named checkpoint in the pipeline so the current expression-state
  can be referenced as a CTE in later expressions.

  Why assignment is a pipe op (not a terminal): keeping '|= name' as a regular
  operation preserves composability. 'company |= x | w: active = true' labels
  company as 'x' (unfiltered) while the pipeline continues to filter. Subsequent
  expressions that use 'x' get the unfiltered company CTE; the current expression
  still produces the filtered SELECT.

  Why a snapshot: storing a dissoc'd copy of state (not the live state) prevents
  the pending-assignments map from accumulating nested copies across multiple
  assignments in a single expression.")

(defn handle
  "Snapshot the current state under varname in :pending-assignments.
  The snapshot excludes :pending-assignments itself to prevent nesting.
  Also registers varname as a local alias for the current SQL alias so
  subsequent ops in the same expression can write e.g. x.id to mean c_0.id."
  [state varname]
  (-> state
      (assoc-in [:pending-assignments varname] (dissoc state :pending-assignments))
      (assoc :assign varname)))
