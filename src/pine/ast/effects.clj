(ns pine.ast.effects
  "Which operations change data.

  The grammar already marks these with `!` - `delete!`, `update!` - but only at
  the surface. This is the same fact available to code, so a caller can ask
  whether an expression writes instead of pattern-matching its text.

  Deliberately narrow: it answers \"does this change data\", nothing else. The
  operation-type sets in ast/select.clj (should-add-auto-ids?) and eval.clj
  (star-eligible?) look similar but ask a different question - whether the
  operation projects real columns - and happen to overlap. Folding them in here
  would tie two unrelated rules together.")

(def writing-ops
  "Operation types that change data."
  #{:delete-action :update-action :update-partial})

(defn writes?
  "Does this operation type change data?"
  [op-type]
  (contains? writing-ops op-type))

(defn any-writes?
  "Does any operation in this sequence of operation types change data?

  Checks every operation, not only the terminal one that decides which query
  gets built. A non-terminal `delete!` is inert today - build-query dispatches
  on the last operation, so `company | delete! .id | select: name` builds a
  SELECT - but that is a property of the current dispatcher, not a promise.
  This is the answer a caller refusing writes needs to be able to rely on."
  [op-types]
  (boolean (some writes? op-types)))
