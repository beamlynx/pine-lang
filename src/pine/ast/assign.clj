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
  assignments in a single expression.

  Ordering dependency: the snapshot must have any auto-id columns added before
  it's stored, not after. get-source-tables (ast/main.clj) decides which tables
  a variable can join to by checking whether a table's own id column is present
  in the snapshot's :columns — it doesn't add one itself, it only ever looks. So
  whatever ends up in :columns by the time this snapshot is taken is final."
  (:require
   [pine.ast.select :as select]))

(defn handle
  "Snapshot the current state under varname in :pending-assignments, with
  auto-id columns added (select/add-auto-id-columns) the same way they'd be
  added to any other query's final result. The snapshot excludes
  :pending-assignments itself to prevent nesting.
  Also registers varname as a local alias for the current SQL alias so
  subsequent ops in the same expression can write e.g. x.id to mean c_0.id."
  [state varname]
  (-> state
      (assoc-in [:pending-assignments varname]
                (-> state (dissoc :pending-assignments) select/add-auto-id-columns))
      (assoc :assign varname)))
