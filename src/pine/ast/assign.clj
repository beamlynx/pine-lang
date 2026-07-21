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

  Why add-auto-id-columns here: post-handle normally adds auto-id columns after
  handle-ops finishes, but a |= snapshot is taken mid-fold, before post-handle
  ever runs — so without this, no snapshot would ever carry an id column, even
  for an ordinary select/where/table with no explicit :s: at all. Applying it
  to the snapshot directly (guarded the same way post-handle's is, via
  should-add-auto-ids? — excluded for :group/:count/:delete-action/:update-action)
  makes a variable's join eligibility match what an equivalent non-variable
  pipeline already gets for free. GROUP is excluded because there's no way to
  silently add an unaggregated id column to a GROUP BY query — see
  get-source-tables in ast/main.clj for how that case is handled instead."
  (:require
   [pine.ast.select :as select]))

(defn handle
  "Snapshot the current state under varname in :pending-assignments.
  The snapshot excludes :pending-assignments itself to prevent nesting, and
  gets auto-id columns added the same way an ordinary (non-variable) pipeline
  would via post-handle — see the namespace docstring for why that doesn't
  already happen by the time a snapshot is taken.
  Also registers varname as a local alias for the current SQL alias so
  subsequent ops in the same expression can write e.g. x.id to mean c_0.id."
  [state varname]
  (-> state
      (assoc-in [:pending-assignments varname]
                (-> state (dissoc :pending-assignments) select/add-auto-id-columns))
      (assoc :assign varname)))
