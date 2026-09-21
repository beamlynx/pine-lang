(ns pine.effects-test
  (:require [clojure.test :refer [deftest is testing]]
            [pine.api :as api]
            [pine.ast.effects :as effects]))

(deftest test-writes?
  (testing "operations that change data"
    (is (effects/writes? :delete-action))
    (is (effects/writes? :update-action))
    (is (effects/writes? :update-partial)))

  (testing "operations that do not"
    (doseq [t [:select :select-partial :table :limit :where :where-partial
               :from :group :order :order-partial :count :assign :paths nil]]
      (is (not (effects/writes? t)) (str t " must not read as a write")))))

(deftest test-any-writes?
  (is (false? (effects/any-writes? [])))
  (is (false? (effects/any-writes? [:table :where :select])))
  (is (true? (effects/any-writes? [:table :where :delete-action])))
  (testing "a write anywhere counts, not only the terminal operation"
    ;; build-query dispatches on the LAST operation, so this expression builds a
    ;; SELECT and the delete! is inert today. That is a property of the current
    ;; dispatcher, not a promise - a caller refusing writes must not depend on it.
    (is (true? (effects/any-writes? [:table :delete-action :select])))))

;; These go through api-eval with the :test sentinel connection rather than
;; calling effects/any-writes? directly: the point is not that the predicate
;; works (above) but that api-eval consults it, and consults it *before*
;; running anything. :test has no connection pool, so a read reaches
;; run-query and fails with "Connection not found" - which is exactly what
;; makes the refusal observable. A refused expression never gets that far.
(deftest test-eval-refuses-writes
  (testing "every write form is refused, including the short aliases"
    ;; The regex this replaces (beamlynx-ui's assertNoDestructiveOperator)
    ;; matched `delete!` only, so `d!`, `update!` and `u!` all got through.
    (doseq [expression ["company | where: id = 1 | delete! .id"
                        "company | where: id = 1 | d! .id"
                        "company | where: id = 1 | update! name = 'x'"
                        "company | where: id = 1 | u! name = 'x'"
                        "company | delete! .id | select: name"]]
      (let [response (api/api-eval [expression] :test [] false)]
        (is (= "write-refused" (:error-type response)) expression)
        (is (true? (:writes response)) expression)
        (is (nil? (:result response))
            (str "must refuse before executing: " expression)))))

  (testing "reads still run with allow-writes false"
    ;; The regression an over-eager check would cause: tripping on any `!` in
    ;; the text would break `where: status != 'x'` and every read like it.
    (doseq [expression ["company | select: name"
                        "company | where: status != 'x' | select: name"
                        "company | count:"]]
      (let [response (api/api-eval [expression] :test [] false)]
        (is (not= "write-refused" (:error-type response)) expression)
        (is (false? (:writes response)) expression))))

  (testing "allow-writes defaults to true, so existing callers are unaffected"
    (let [response (api/api-eval ["company | where: id = 1 | delete! .id"] :test [])]
      (is (not= "write-refused" (:error-type response)))
      (is (true? (:writes response))))))
