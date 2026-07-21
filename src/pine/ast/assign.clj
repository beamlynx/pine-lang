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

  Why join-key preservation here, and why it's NOT select/add-auto-id-columns:
  once a snapshot is sealed into a CTE, the outer query can no longer see its
  underlying real tables directly — only whatever the CTE's own :columns select.
  A raw table join never needs an id column in SELECT (a JOIN ... ON clause can
  reference any column of a real table regardless of what's selected), but a CTE
  is different in kind: its column list IS its entire visible schema. So a table
  is only actually joinable through a sealed variable if its own id survived into
  that snapshot's :columns.

  This is unrelated to select/add-auto-id-columns, which exists purely so the UI
  can identify which row to update (see result-updates.md) — a different concern
  that happens to also add an id column, for different reasons, at a different
  time (post-handle, on the final returned state, not on a mid-fold snapshot).
  Coupling the two would mean a future change to update-tracking (e.g. making it
  opt-out) could silently break variable joins for a completely unrelated reason.
  preserve-join-keys is deliberately its own mechanism: its columns are marked
  :hidden (excluded from column hints, same as auto-id) but NOT :auto-id, so nothing
  downstream can conflate 'has a join key' with 'is tracked for updates'.

  GROUP is excluded (see preserve-join-keys): there's no way to silently add an
  unaggregated id column to a GROUP BY without changing what's grouped by. See
  get-source-tables in ast/main.clj, which is what actually enforces that a
  GROUP-sourced variable needs its id explicitly grouped by to be joinable."
  (:require
   [pine.ast.select :as select]))

(defn- create-join-key-column
  "A hidden column that exists purely so a sealed CTE can still be joined via id.
  Deliberately not shaped like select/create-auto-id-column's output (no
  :column-alias, not marked :auto-id) — this is a different mechanism for a
  different purpose; see the namespace docstring."
  [alias operation-index]
  {:column "id" :alias alias :hidden true :join-key true :operation-index operation-index})

(defn- has-explicit-id?
  [columns alias]
  (some #(and (= alias (:alias %)) (= "id" (:column %))) columns))

(defn preserve-join-keys
  "Ensure a state that's about to be sealed into a CTE keeps enough of an id
  column, per real table, to remain joinable once sealing hides its underlying
  tables from the outer query. No-op when :columns is empty (an implicit '*'
  already includes id, regardless of operation type) or when this is a GROUP
  (aggregation means an id can only survive if the user explicitly grouped by
  it — silently adding one here would change what's being grouped by)."
  [state]
  (let [explicit (remove :hidden (:columns state))]
    (if (or (seq (:group state)) (empty? explicit))
      state
      (let [table-aliases (map :alias (:tables state))
            next-operation-index (inc (:index state))
            references (:references state)
            aliases (:aliases state)
            valid-aliases (filter #(and (not (:ast (get aliases %)))
                                        (not (has-explicit-id? (:columns state) %))
                                        (select/has-id-column? references aliases %))
                                  table-aliases)
            join-key-columns (map-indexed #(create-join-key-column %2 (+ next-operation-index %1))
                                          valid-aliases)]
        (update state :columns into join-key-columns)))))

(defn snapshot
  "Prepare state to be stored as a CTE-backing snapshot — used both for a |=
  assignment (below) and for a checkpoint's auto-named seal (flush-checkpoint
  in ast/main.clj), the two places a piece of pipeline state gets frozen for
  later reference as a CTE. Drops :pending-assignments (preventing nested
  accumulation across multiple assignments in one expression) and preserves
  join keys (see preserve-join-keys)."
  [state]
  (-> state (dissoc :pending-assignments) preserve-join-keys))

(defn handle
  "Snapshot the current state under varname in :pending-assignments (see
  snapshot above for what that entails).
  Also registers varname as a local alias for the current SQL alias so
  subsequent ops in the same expression can write e.g. x.id to mean c_0.id."
  [state varname]
  (-> state
      (assoc-in [:pending-assignments varname] (snapshot state))
      (assoc :assign varname)))
