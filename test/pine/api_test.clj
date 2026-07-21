(ns pine.api-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
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

(deftest test-build-ast
  (let [state (generate-with-variables
               ["tenant as t | company .tenantId | group: t.title |= x"
                "x | s: count, | o: count desc |= y"
                "y | s: count, |= z"
                "z | "])
        built-ast (#'api/build-ast state)]

    (testing "ast.variables entries are trimmed like pending-assignments, not raw snapshots"
      ;; A raw variable snapshot carries :variables and :references from
      ;; pre-handle/post-handle. Left untrimmed, each chained |= re-embeds every
      ;; earlier variable's own full snapshot inside the new one, growing the
      ;; response payload superlinearly with the number of chained expressions
      ;; instead of linearly.
      (is (= #{"x" "y" "z"} (set (keys (:variables built-ast)))))
      (doseq [[name var-ast] (:variables built-ast)]
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
      (doseq [table (:selected-tables built-ast)]
        (assert-clean-table table))
      (doseq [[_name var-ast] (:variables built-ast)
              table (concat (:tables var-ast) (:selected-tables var-ast))]
        (assert-clean-table table)))

    (testing "payload stays flat as more expressions chain, not superlinear"
      ;; Regression guard: before the fix this was ~285KB for this 4-expression
      ;; chain (each block re-embedding every prior block's full state); it
      ;; should stay in the same ballpark regardless of chain length.
      (is (< (count (json/generate-string built-ast)) 5000)))))
