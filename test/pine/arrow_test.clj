(ns pine.arrow-test
  "The arrow applies a function, `x => f` is f(x) and `x => f(a)` is f(x, a),
  and `with` divides a group's keys from its aggregates. See
  docs/computed-values.md."
  (:require [clojure.string :as s]
            [clojure.test :refer [deftest is testing]]
            [pine.ast.main :as ast]
            [pine.computed :as computed]
            [pine.eval :as eval]
            [pine.parser :as parser]
            [pine.sql.transpile :as t]))

(defn- sql [expression]
  (-> expression parser/parse :result (ast/generate :test nil nil {} nil) eval/build-query eval/formatted-query s/trim))

(defn- error [expression]
  (try (let [{:keys [error]} (parser/parse expression)]
         (or error (do (sql expression) nil)))
       (catch clojure.lang.ExceptionInfo e (.getMessage e))))

(deftest test-arrow-is-a-call
  (testing "x => f is f(x)"
    (is (= (sql "employee | s: id, created_at => year => concat('!') as k")
           (sql "employee | s: id, concat(year(created_at), '!') as k"))))

  (testing "x => f(a, b) is f(x, a, b)"
    (is (= (sql "report | s: title => coalesce(id::text, 'none') as k")
           (sql "report | s: coalesce(title, id::text, 'none') as k"))))

  (testing "steps chain left to right"
    (is (= (sql "report | s: title => coalesce('x') => concat('!') as k")
           (sql "report | s: concat(coalesce(title, 'x'), '!') as k"))))

  (testing "a date bucket is a function too"
    (is (s/includes? (sql "employee | s: id, month(created_at) as m")
                     "TO_CHAR(DATE_TRUNC('month', \"e_0\".\"created_at\"), 'YYYY-MM') AS \"m\""))
    (is (s/includes? (sql "employee | s: created_at => month")
                     "TO_CHAR(DATE_TRUNC('month', \"e_0\".\"created_at\"), 'YYYY-MM') AS \"month\"")))

  (testing "an aggregate outside group: says where it goes"
    (is (s/includes? (error "employee | s: id => min as m") "Use it after `with` in group:")))

  (testing "a condition before the arrow is a parse error that says what to write"
    (is (s/includes? (error "employee | s: id = 1 => any as m")
                     "A condition before `=>` goes in parentheses, `(type = 'x') => any`, or is written as a call"))))

(deftest test-group-with
  (testing "group: status counts each group, as group: status with count does"
    (is (= (sql "company | group: name") (sql "company | group: name with count"))))

  (testing "an aggregate takes the arrow or the call, with a filter and a name"
    (is (= (sql "company | group: name with id => max as top, id => min where name = 'x' as first")
           (sql "company | group: name with max(id) as top, min(id) where name = 'x' as first"))))

  (testing "a condition aggregate takes parentheses or the call"
    (is (= (sql "company | group: name with (id = 1) => any as has_one")
           (sql "company | group: name with any(id = 1) as has_one"))))

  (testing "`=>` after the keys names the new form"
    (is (s/includes? (error "company | group: name => count")
                     "Write `group: status with count`, or just `group: status`")))

  (testing "an aggregate needs a value"
    (is (s/includes? (error "company | group: name with sum") "`sum` needs a value: `x => sum`")))

  (testing "a value that isn't an aggregate is refused after with"
    (is (s/includes? (error "company | group: name with id") "After `with` in group: come aggregates"))))

(deftest test-pine-written-by-pine
  (testing "the printer: one value takes the arrow, anything else the call"
    (is (= "a.id => month" (computed/text {:kind :call :fn "month" :args [{:kind :column :alias "a" :column "id"}]})))
    (is (= "coalesce(a, b)" (computed/text {:kind :call :fn "coalesce" :args [{:kind :column :column "a"} {:kind :column :column "b"}]})))
    (is (= "a.id::text => min where type = 'x'"
           (computed/text {:kind :aggregate :fn "min" :arg {:kind :column :alias "a" :column "id" :cast "text"}
                           :filter {:kind :condition :conditions [{:column "type" :operator "=" :value {:type :string :value "x"}}]}})))
    (is (= "any(type = 'x')"
           (computed/text {:kind :aggregate :fn "any"
                           :arg {:kind :condition :conditions [{:column "type" :operator "=" :value {:type :string :value "x"}}]}}))))

  (testing "SQL to Pine writes with, the arrow for one value, and the call for a condition"
    (is (= "t\n | group: a with count, b => min as first_b, any(c = 1) as any_c"
           (:expression (t/sql->pine "select a, count(*), min(b) as first_b, bool_or(c = 1) as any_c from t group by a"))))))
