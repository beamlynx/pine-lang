(ns pine.api-test
  (:require [clojure.test :refer [deftest is testing]]
            [pine.api :as api]))

(defn- assert-clean-table [table]
  (is (= #{:schema :table :alias} (set (keys table)))
      "table entries must not carry :ast (a variable's own full state snapshot)"))

(deftest test-api-build-empty-expression
  (testing "an empty/blank last expression still returns table hints instead of short-circuiting"
    ;; Regression test: api-build used to short-circuit on any blank last-expr
    ;; (str/blank?), which also swallowed "" - even though only nil actually
    ;; crashes the parser. That meant pressing Tab on an empty input showed
    ;; no table hints at all.
    (doseq [expressions [[] [""] ["   "]]]
      (let [response (api/api-build expressions nil :test)]
        (is (nil? (:error response)))
        (is (seq (get-in response [:ast :hints :table]))
            (str "expected table hints for expressions " (pr-str expressions)))))))

;; These call the real public entry point (api/api-build), not the private
;; prune-ast helper directly. Testing prune-ast in isolation only proves the
;; helper itself behaves correctly — it proves nothing about whether api-build
;; still routes its result through prune-ast at all, so a future edit that
;; forgets to call it, passes the wrong state, or leaks a new field through
;; some other path would go uncaught. Calling api-build with connection-id
;; :test is what makes this possible: :test is a shared sentinel
;; (pine.db.connections/test-connection-id) that both the schema lookup
;; (postgres.clj) and the connection-name lookup (connections.clj) recognize,
;; so api-build's connections/get-connection-name call — which normally
;; requires a real registered connection pool — succeeds without one.
(deftest test-api-build-ast
  (let [single-block  (:ast (api/api-build
                             ["tenant as t | company .tenantId | group: t.title |= x"]
                             nil :test))
        chained-blocks (:ast (api/api-build
                              ["tenant as t | company .tenantId | group: t.title |= x"
                               "x | s: count, | o: count desc |= y"
                               "y | s: count, |= z"
                               "z | "]
                              nil :test))]

    (testing "ast.variables entries are pruned like pending-assignments, not raw snapshots"
      ;; A raw variable snapshot carries :variables and :references from
      ;; pre-handle/post-handle. Left unpruned, each chained |= re-embeds every
      ;; earlier variable's own full snapshot inside the new one, growing the
      ;; response payload superlinearly with the number of chained expressions
      ;; instead of linearly.
      (is (= #{"x" "y" "z"} (set (keys (:variables chained-blocks)))))
      (doseq [[name var-ast] (:variables chained-blocks)]
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
      (doseq [table (:selected-tables chained-blocks)]
        (assert-clean-table table))
      (doseq [[_name var-ast] (:variables chained-blocks)
              table (concat (:tables var-ast) (:selected-tables var-ast))]
        (assert-clean-table table)))

    (testing "an earlier variable's own entry is unaffected by how many blocks chain after it"
      ;; The actual bug wasn't about absolute size (which is arbitrary and brittle to
      ;; pin to a byte count) — it was that x's entry kept growing every time another
      ;; block chained onto it. Pruning removes the machinery (:variables/:references/
      ;; :ast) that let that happen, so x's pruned entry here should be byte-for-byte
      ;; identical whether it's standing alone or three more blocks have chained onto
      ;; it since.
      (is (= (get-in single-block [:pending-assignments "x"])
             (get-in chained-blocks [:variables "x"]))))))
