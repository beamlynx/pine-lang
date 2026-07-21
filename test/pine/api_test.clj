(ns pine.api-test
  (:require [clojure.test :refer [deftest is testing]]
            [pine.api :as api]
            [pine.ast.main :as ast]
            [pine.parser :as parser]))

(defn- generate-with-variables
  "Evaluate a sequence of expressions sequentially, threading variables between
  them the same way api-build's evaluate-expressions does. Returns the final state."
  [expressions]
  (:last-state
   (reduce (fn [{:keys [variables]} expr]
             (let [{:keys [result]} (parser/parse expr)
                   state (ast/generate result :test nil nil variables)]
               {:variables (merge variables (:pending-assignments state))
                :last-state state}))
           {:variables {} :last-state nil}
           expressions)))

(defn- assert-clean-table [table]
  (is (= #{:schema :table :alias} (set (keys table)))
      "table entries must not carry :ast (a variable's own full state snapshot)"))

(deftest test-prune-ast
  (let [single-block  (generate-with-variables
                       ["tenant as t | company .tenantId | group: t.title |= x"])
        chained-blocks (generate-with-variables
                        ["tenant as t | company .tenantId | group: t.title |= x"
                         "x | s: count, | o: count desc |= y"
                         "y | s: count, |= z"
                         "z | "])
        single-ast  (#'api/prune-ast single-block)
        chained-ast (#'api/prune-ast chained-blocks)]

    (testing "ast.variables entries are pruned like pending-assignments, not raw snapshots"
      ;; A raw variable snapshot carries :variables and :references from
      ;; pre-handle/post-handle. Left unpruned, each chained |= re-embeds every
      ;; earlier variable's own full snapshot inside the new one, growing the
      ;; response payload superlinearly with the number of chained expressions
      ;; instead of linearly.
      (is (= #{"x" "y" "z"} (set (keys (:variables chained-ast)))))
      (doseq [[name var-ast] (:variables chained-ast)]
        (testing (str "variable " name)
          (is (= #{:tables :selected-tables :joins :columns} (set (keys var-ast)))
              "should only carry the fields VariableAst (client.ts) actually uses")
          (is (not (contains? var-ast :variables))
              "must not recursively embed earlier variables' own snapshots")
          (is (not (contains? var-ast :references))
              "must not carry the full schema references map"))))

    (testing "table entries (top-level and nested inside variables) never carry :ast"
      ;; A variable-backed table entry carries a full :ast (the variable's own
      ;; var-ast) for the query builder's CTE generation. Left in place, that
      ;; recursively re-embeds the variable's entire state — and everything IT
      ;; wraps in turn — inside every table list that references it, one level
      ;; down from the :variables map itself.
      (doseq [table (:selected-tables chained-ast)]
        (assert-clean-table table))
      (doseq [[_name var-ast] (:variables chained-ast)
              table (concat (:tables var-ast) (:selected-tables var-ast))]
        (assert-clean-table table)))

    (testing "an earlier variable's own entry is unaffected by how many blocks chain after it"
      ;; The actual bug wasn't about absolute size (which is arbitrary and brittle to
      ;; pin to a byte count) — it was that x's entry kept growing every time another
      ;; block chained onto it. Pruning removes the machinery (:variables/:references/
      ;; :ast) that let that happen, so x's pruned entry here should be byte-for-byte
      ;; identical whether it's standing alone or three more blocks have chained onto
      ;; it since.
      (is (= (get-in single-ast [:pending-assignments "x"])
             (get-in chained-ast [:variables "x"]))))))
