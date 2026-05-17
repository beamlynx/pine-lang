(ns pine.eval-test
  (:require [clojure.test :refer [deftest is testing]]
            [pine.ast.main :as ast]
            [pine.parser :as parser]
            [pine.eval :as eval]
            [pine.data-types :as dt]))

(defn- generate
  "Helper function to generate the sql"
  [expression]
  (-> expression
      parser/parse
      :result
      (ast/generate :test)
      eval/build-query))

(defn- generate-expressions
  "Evaluate a list of pine expressions sequentially, threading variables.
  Returns the SQL of the last expression."
  [expressions]
  (let [{:keys [last-state]}
        (reduce (fn [{:keys [variables]} expr]
                  (let [{:keys [result]} (parser/parse expr)
                        state (ast/generate result :test nil nil variables)]
                    {:variables (merge variables (:pending-assignments state))
                     :last-state state}))
                {:variables {} :last-state nil}
                expressions)]
    (eval/build-query last-state)))

(deftest test-build-query

  (testing "qualify table"
    (is (= "\"x\"" (eval/q "x")))
    (is (= "\"x\".\"y\"" (eval/q "x" "y"))))

  (testing "No expression"
    (is (= {:query "",
            :params nil}
           (generate ""))))

  (testing "Select"
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" LIMIT 250",
            :params nil}
           (generate "company")))
    (is (= {:query "SELECT \"c_0\".\"id\", \"c_0\".id AS \"__c_0__id\" FROM \"company\" AS \"c_0\" LIMIT 1",
            :params nil}
           (generate "company | s: id, | l: 1")))
    (is (= {:query "SELECT \"c_0\".\"id\", \"c_0\".id AS \"__c_0__id\" FROM \"company\" AS \"c_0\" LIMIT 1",
            :params nil}
           (generate "company | s: id | l: 1")))
    (is (= {:query "SELECT \"c\".\"name\", \"e\".\"name\", \"c\".id AS \"__c__id\", \"e\".id AS \"__e__id\" FROM \"company\" AS \"c\" JOIN \"employee\" AS \"e\" ON \"c\".\"id\" = \"e\".\"company_id\" LIMIT 250",
            :params nil}
           (generate "company as c | s: name | employee as e | s: name"))))

  (testing "Count"
    (is (= {:query "WITH x AS ( SELECT \"c_0\".* FROM \"company\" AS \"c_0\" ) SELECT COUNT(*) FROM x",
            :params nil}
           (generate "company | count:")))
    (is (= {:query "WITH x AS ( SELECT \"c_0\".* FROM \"company\" AS \"c_0\" LIMIT 100 ) SELECT COUNT(*) FROM x",
            :params nil}
           (generate "company | limit: 100 | count:"))))

  (testing "Condition : ="
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"name\" = ? LIMIT 250",
            :params (map dt/string ["Acme Inc."])}
           (generate "company | where: name='Acme Inc.'")))
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"name\" LIKE ? AND \"c_0\".\"country\" = ? LIMIT 250",
            :params (map dt/string ["Acme%", "PK"])}
           (generate "company | where: name like 'Acme%' | country = 'PK'")))
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"name\" NOT LIKE ? LIMIT 250",
            :params (map dt/string ["Acme%"])}
           (generate "company | where: name not like 'Acme%'")))
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"name\" ILIKE ? LIMIT 250",
            :params (map dt/string ["acme%"])}
           (generate "company | where: name ilike 'acme%'")))
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"name\" NOT ILIKE ? LIMIT 250",
            :params (map dt/string ["acme%"])}
           (generate "company | where: name not ilike 'acme%'")))
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"id\" = ? LIMIT 250",
            :params (map dt/number ["1"])}
           (generate "company | where: id = 1")))
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"id\" != ? LIMIT 250",
            :params (map dt/number ["1"])}
           (generate "company | where: id != 1")))
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"id\" IS NULL LIMIT 250",
            :params nil}
           (generate "company | where: id is null"))))

  (testing "Condition : !="
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"name\" != ? LIMIT 250",
            :params (map dt/string ["Acme Inc."])}
           (generate "company | where: name != 'Acme Inc.'"))))

  (testing "Condition : IN"
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"country\" IN (?, ?) LIMIT 250",
            :params (map dt/string ["PK", "DK"])}
           (generate "company | where: country in ('PK' 'DK')"))))

  (testing "Condition : columns"
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"name\" = \"country\" LIMIT 250",
            :params nil}
           (generate "company | where: name = country")))
    (is (= {:query "SELECT \"c\".id AS \"__c__id\", \"c\".* FROM \"company\" AS \"c\" WHERE \"c\".\"name\" != \"c\".\"country\" LIMIT 250",
            :params nil}
           (generate "company as c | name != c.country")))
    (is (= {:query "SELECT \"c\".id AS \"__c__id\", \"c\".* FROM \"company\" AS \"c\" WHERE \"c\".\"name\" != \"c\".\"country\" LIMIT 250",
            :params nil}
           (generate "company as c | c.name != c.country"))))

  (testing "Condition : NULL"
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"deleted_at\" IS NULL LIMIT 250",
            :params nil}
           (generate "company | where: deleted_at is null")))
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"deleted_at\" IS NOT NULL LIMIT 250",
            :params nil}
           (generate "company | where: deleted_at is not null")))
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"deleted_at\" IS NULL LIMIT 250",
            :params nil}
           (generate "company | where: deleted_at = null"))))

  (testing "Condition with cast"
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"name\"::text = ? LIMIT 250",
            :params (map dt/string ["Acme Inc."])}
           (generate "company | where: name = 'Acme Inc.' ::text")))
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"id\"::uuid = ? LIMIT 250",
            :params (map dt/string ["123e4567-e89b-12d3-a456-426614174000"])}
           (generate "company | where: id = '123e4567-e89b-12d3-a456-426614174000' ::uuid"))))

  (testing "Joins"
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"e_1\".id AS \"__e_1__id\", \"e_1\".* FROM \"company\" AS \"c_0\" JOIN \"employee\" AS \"e_1\" ON \"c_0\".\"id\" = \"e_1\".\"company_id\" LIMIT 250",
            :params nil}
           (generate "company | employee")))
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"e_1\".id AS \"__e_1__id\", \"e_1\".* FROM \"x\".\"company\" AS \"c_0\" JOIN \"y\".\"employee\" AS \"e_1\" ON \"c_0\".\"id\" = \"e_1\".\"company_id\" LIMIT 250",
            :params nil}
           (generate "x.company | y.employee")))
    (is (= {:query "SELECT \"e_0\".id AS \"__e_0__id\", \"c_1\".id AS \"__c_1__id\", \"c_1\".* FROM \"y\".\"employee\" AS \"e_0\" JOIN \"x\".\"company\" AS \"c_1\" ON \"e_0\".\"company_id\" = \"c_1\".\"id\" LIMIT 250",
            :params nil}
           (generate "y.employee | x.company")))
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"e_1\".id AS \"__e_1__id\", \"d_2\".id AS \"__d_2__id\", \"d_2\".* FROM \"x\".\"company\" AS \"c_0\" JOIN \"y\".\"employee\" AS \"e_1\" ON \"c_0\".\"id\" = \"e_1\".\"company_id\" JOIN \"z\".\"document\" AS \"d_2\" ON \"e_1\".\"id\" = \"d_2\".\"employee_id\" LIMIT 250",
            :params nil}
           (generate "x.company | y.employee | z.document"))))

  (testing "Joins with join types"
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"e_1\".id AS \"__e_1__id\", \"e_1\".* FROM \"company\" AS \"c_0\" LEFT JOIN \"employee\" AS \"e_1\" ON \"c_0\".\"id\" = \"e_1\".\"company_id\" LIMIT 250",
            :params nil}
           (generate "company | employee :left")))
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"e_1\".id AS \"__e_1__id\", \"e_1\".* FROM \"company\" AS \"c_0\" RIGHT JOIN \"employee\" AS \"e_1\" ON \"c_0\".\"id\" = \"e_1\".\"company_id\" LIMIT 250",
            :params nil}
           (generate "company | employee :right")))
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"e_1\".id AS \"__e_1__id\", \"d_2\".id AS \"__d_2__id\", \"d_2\".* FROM \"x\".\"company\" AS \"c_0\" LEFT JOIN \"y\".\"employee\" AS \"e_1\" ON \"c_0\".\"id\" = \"e_1\".\"company_id\" RIGHT JOIN \"z\".\"document\" AS \"d_2\" ON \"e_1\".\"id\" = \"d_2\".\"employee_id\" LIMIT 250",
            :params nil}
           (generate "x.company | y.employee :left | z.document :right")))
    ;; Self-join: :parent flips join direction (e_0 has the FK, not e_1)
    (is (true? (clojure.string/includes?
                (:query (generate "employee | employee :parent"))
                "ON \"e_0\".\"reports_to\" = \"e_1\".\"id\""))))

  (testing "Joins with explicit columns"
    ;; Basic explicit join (tables a, b, c don't exist in schema so no auto-id columns)
    (is (= {:query "SELECT \"b_1\".* FROM \"a\" AS \"a_0\" JOIN \"b\" AS \"b_1\" ON \"a_0\".\"id\" = \"b_1\".\"a_id\" LIMIT 250",
            :params nil}
           (generate "a | b .a_id = .id")))

    ;; With real tables
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"e_1\".id AS \"__e_1__id\", \"e_1\".* FROM \"company\" AS \"c_0\" JOIN \"employee\" AS \"e_1\" ON \"c_0\".\"id\" = \"e_1\".\"company_id\" LIMIT 250",
            :params nil}
           (generate "company | employee .company_id = .id")))

    ;; With different column names (tables don't exist in schema)
    (is (= {:query "SELECT \"b_1\".* FROM \"a\" AS \"a_0\" JOIN \"b\" AS \"b_1\" ON \"a_0\".\"custom_id\" = \"b_1\".\"foreign_id\" LIMIT 250",
            :params nil}
           (generate "a | b .foreign_id = .custom_id")))

    ;; With LEFT JOIN
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"e_1\".id AS \"__e_1__id\", \"e_1\".* FROM \"company\" AS \"c_0\" LEFT JOIN \"employee\" AS \"e_1\" ON \"c_0\".\"id\" = \"e_1\".\"company_id\" LIMIT 250",
            :params nil}
           (generate "company | employee .company_id = .id :left")))

    ;; With RIGHT JOIN
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"e_1\".id AS \"__e_1__id\", \"e_1\".* FROM \"company\" AS \"c_0\" RIGHT JOIN \"employee\" AS \"e_1\" ON \"c_0\".\"id\" = \"e_1\".\"company_id\" LIMIT 250",
            :params nil}
           (generate "company | employee .company_id = .id :right")))

    ;; With schema-qualified tables
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"e_1\".id AS \"__e_1__id\", \"e_1\".* FROM \"x\".\"company\" AS \"c_0\" JOIN \"y\".\"employee\" AS \"e_1\" ON \"c_0\".\"id\" = \"e_1\".\"company_id\" LIMIT 250",
            :params nil}
           (generate "x.company | y.employee .company_id = .id")))

    ;; Multiple joins with explicit columns (tables don't exist in schema)
    (is (= {:query "SELECT \"c_2\".* FROM \"a\" AS \"a_0\" JOIN \"b\" AS \"b_1\" ON \"a_0\".\"id\" = \"b_1\".\"a_id\" JOIN \"c\" AS \"c_2\" ON \"b_1\".\"id\" = \"c_2\".\"b_id\" LIMIT 250",
            :params nil}
           (generate "a | b .a_id = .id | c .b_id = .id"))))

  (testing "Joins with a context"
    (is (= {:query "SELECT \"c\".id AS \"__c__id\", \"e_1\".id AS \"__e_1__id\", \"d_2\".id AS \"__d_2__id\", \"d_2\".* FROM \"x\".\"company\" AS \"c\" JOIN \"y\".\"employee\" AS \"e_1\" ON \"c\".\"id\" = \"e_1\".\"company_id\" JOIN \"z\".\"document\" AS \"d_2\" ON \"c\".\"id\" = \"d_2\".\"company_id\" LIMIT 250",
            :params nil}
           (generate "x.company as c | y.employee | from: c | z.document"))))

  (testing "order"
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" ORDER BY \"c_0\".\"country\" DESC LIMIT 250",
            :params nil}
           (generate "company | order: country")))
    ;; Test aliased order columns
    (is (= {:query "SELECT \"e\".id AS \"__e__id\", \"e\".* FROM \"employee\" AS \"e\" ORDER BY \"e\".\"name\" DESC LIMIT 250",
            :params nil}
           (generate "employee as e | order: e.name")))
    (is (= {:query "SELECT \"e\".id AS \"__e__id\", \"e\".* FROM \"employee\" AS \"e\" ORDER BY \"e\".\"name\" ASC LIMIT 250",
            :params nil}
           (generate "employee as e | order: e.name asc")))
    (is (= {:query "SELECT \"e\".id AS \"__e__id\", \"e\".* FROM \"employee\" AS \"e\" ORDER BY \"e\".\"name\" DESC, \"e\".\"created_at\" ASC LIMIT 250",
            :params nil}
           (generate "employee as e | order: e.name, e.created_at asc")))

    ;; Test mixed aliased and non-aliased order columns
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" ORDER BY \"c_0\".\"name\" DESC, \"c_0\".\"age\" DESC LIMIT 250",
            :params nil}
           (generate "company | order: name desc, age desc")))
    (is (= {:query "SELECT \"e\".id AS \"__e__id\", \"d_1\".id AS \"__d_1__id\", \"d_1\".* FROM \"employee\" AS \"e\" JOIN \"document\" AS \"d_1\" ON \"e\".\"id\" = \"d_1\".\"employee_id\" ORDER BY \"e\".\"name\" DESC, \"d_1\".\"title\" ASC LIMIT 250",
            :params nil}
           (generate "employee as e | document | order: e.name desc, title asc"))))

  (testing "columns"
    (is (= {:query "SELECT \"c\".\"id\", \"c\".id AS \"__c__id\" FROM \"company\" AS \"c\" LIMIT 250",
            :params nil}
           (generate "company as c | select: id")))
    (is (= {:query "SELECT \"c_0\".\"id\", \"c_0\".id AS \"__c_0__id\" FROM \"company\" AS \"c_0\" LIMIT 250",
            :params nil}
           (generate "company | select: id")))
    (is (= {:query "SELECT \"c_0\".\"id\" AS \"c_id\", \"c_0\".id AS \"__c_0__id\" FROM \"company\" AS \"c_0\" LIMIT 250",
            :params nil}
           (generate "company | select: id as c_id")))
    (is (= {:query "SELECT \"c_0\".\"id\", \"c_0\".id AS \"__c_0__id\", \"e_1\".id AS \"__e_1__id\", \"e_1\".* FROM \"company\" AS \"c_0\" JOIN \"employee\" AS \"e_1\" ON \"c_0\".\"id\" = \"e_1\".\"company_id\" LIMIT 250",
            :params nil}
           (generate "company | select: id | employee")))
    (is (= {:query "SELECT \"c_0\".\"id\", \"e_1\".\"id\", \"c_0\".id AS \"__c_0__id\", \"e_1\".id AS \"__e_1__id\" FROM \"company\" AS \"c_0\" JOIN \"employee\" AS \"e_1\" ON \"c_0\".\"id\" = \"e_1\".\"company_id\" LIMIT 250",
            :params nil}
           (generate "company | s: id | employee | s: id")))
    (is (= {:query "SELECT \"c\".\"id\", \"e\".*, \"c\".id AS \"__c__id\", \"e\".id AS \"__e__id\" FROM \"company\" AS \"c\" JOIN \"employee\" AS \"e\" ON \"c\".\"id\" = \"e\".\"company_id\" LIMIT 250",
            :params nil}
           (generate "company as c | employee as e | s: c.id, e.*"))))

  (testing "group"
    (is (= {:query "WITH \"x_1\" AS ( SELECT \"e_0\".\"status\" AS \"status\" FROM \"email\" AS \"e_0\" ) SELECT \"x_1\".\"status\", COUNT(1) AS \"count\" FROM \"x_1\" GROUP BY \"x_1\".\"status\"",
            :params nil}
           (generate "email | group: status => count"))))

  (testing "date extraction functions"
    ;; Year extraction
    (is (= {:query "SELECT TO_CHAR(DATE_TRUNC('year', \"e_0\".\"created_at\"), 'YYYY') AS \"year\", \"e_0\".id AS \"__e_0__id\" FROM \"employee\" AS \"e_0\" LIMIT 250",
            :params nil}
           (generate "employee | select: created_at => year")))

    ;; Month extraction
    (is (= {:query "SELECT TO_CHAR(DATE_TRUNC('month', \"e_0\".\"created_at\"), 'YYYY-MM') AS \"month\", \"e_0\".id AS \"__e_0__id\" FROM \"employee\" AS \"e_0\" LIMIT 250",
            :params nil}
           (generate "employee | select: created_at => month")))

    ;; Day extraction
    (is (= {:query "SELECT TO_CHAR(DATE_TRUNC('day', \"e_0\".\"created_at\"), 'YYYY-MM-DD') AS \"day\", \"e_0\".id AS \"__e_0__id\" FROM \"employee\" AS \"e_0\" LIMIT 250",
            :params nil}
           (generate "employee | select: created_at => day")))

    ;; Week extraction
    (is (= {:query "SELECT TO_CHAR(DATE_TRUNC('week', \"e_0\".\"created_at\"), 'YYYY-MM-DD') AS \"week\", \"e_0\".id AS \"__e_0__id\" FROM \"employee\" AS \"e_0\" LIMIT 250",
            :params nil}
           (generate "employee | select: created_at => week")))

    ;; Hour extraction (uses timestamp)
    (is (= {:query "SELECT TO_CHAR(DATE_TRUNC('hour', \"e_0\".\"created_at\"), 'YYYY-MM-DD HH24') AS \"hour\", \"e_0\".id AS \"__e_0__id\" FROM \"employee\" AS \"e_0\" LIMIT 250",
            :params nil}
           (generate "employee | select: created_at => hour")))

    ;; With table alias
    (is (= {:query "SELECT TO_CHAR(DATE_TRUNC('month', \"e\".\"created_at\"), 'YYYY-MM') AS \"month\", \"e\".id AS \"__e__id\" FROM \"employee\" AS \"e\" LIMIT 250",
            :params nil}
           (generate "employee as e | select: e.created_at => month")))

    ;; With custom column alias
    (is (= {:query "SELECT TO_CHAR(DATE_TRUNC('month', \"e_0\".\"created_at\"), 'YYYY-MM') AS \"created_at_month\", \"e_0\".id AS \"__e_0__id\" FROM \"employee\" AS \"e_0\" LIMIT 250",
            :params nil}
           (generate "employee | select: created_at => month as created_at_month")))

    ;; Mixed with regular columns
    (is (= {:query "SELECT \"e_0\".\"name\", TO_CHAR(DATE_TRUNC('year', \"e_0\".\"created_at\"), 'YYYY') AS \"year\", \"e_0\".id AS \"__e_0__id\" FROM \"employee\" AS \"e_0\" LIMIT 250",
            :params nil}
           (generate "employee | select: name, created_at => year"))))

  (testing "date extraction with grouping"
    ;; Group by name AND month extraction (both specified in group)
    (is (= {:query "WITH \"x_2\" AS ( SELECT \"e_0\".\"name\" AS \"name\", TO_CHAR(DATE_TRUNC('month', \"e_0\".\"created_at\"), 'YYYY-MM') AS \"month\" FROM \"employee\" AS \"e_0\" ) SELECT \"x_2\".\"name\", \"x_2\".\"month\", COUNT(1) AS \"count\" FROM \"x_2\" GROUP BY \"x_2\".\"name\", \"x_2\".\"month\"",
            :params nil}
           (generate "employee | select: name, created_at => month | group: name, created_at => count")))

    ;; Group by just the extracted date (only month in group, month also selected)
    (is (= {:query "WITH \"x_2\" AS ( SELECT TO_CHAR(DATE_TRUNC('month', \"e_0\".\"created_at\"), 'YYYY-MM') AS \"month\" FROM \"employee\" AS \"e_0\" ) SELECT \"x_2\".\"month\", COUNT(1) AS \"count\" FROM \"x_2\" GROUP BY \"x_2\".\"month\"",
            :params nil}
           (generate "employee | select: created_at => month | group: created_at => count")))

    ;; Select multiple columns but group by only one (month)
    (is (= {:query "WITH \"x_2\" AS ( SELECT TO_CHAR(DATE_TRUNC('month', \"e_0\".\"created_at\"), 'YYYY-MM') AS \"month\" FROM \"employee\" AS \"e_0\" ) SELECT \"x_2\".\"month\", COUNT(1) AS \"count\" FROM \"x_2\" GROUP BY \"x_2\".\"month\"",
            :params nil}
           (generate "employee | select: name, created_at => month | group: month => count"))))

  (testing "delete action"
    (is (= {:query "DELETE FROM \"company\" WHERE \"id\" IN ( SELECT \"c_0\".\"id\" FROM \"company\" AS \"c_0\" )",
            :params nil}
           (generate "company | delete! .id"))))

  (testing "update action"
    (is (= {:queries [{:table "company"
                       :query "UPDATE \"company\" SET \"name\" = ? WHERE id IN ( SELECT \"c_0\".\"id\" FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"id\" = ? )"
                       :params (list (dt/string "John Doe") (dt/number "1"))}]}
           (generate "company | where: id = 1 | update! name = 'John Doe'")))
    (is (= {:queries [{:table "company"
                       :query "UPDATE \"company\" SET \"name\" = ?, \"age\" = ? WHERE id IN ( SELECT \"c_0\".\"id\" FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"id\" = ? )"
                       :params (list (dt/string "John") (dt/number "30") (dt/number "1"))}]}
           (generate "company | where: id = 1 | update! name = 'John', age = 30")))
    (is (= {:queries [{:table "company"
                       :query "UPDATE \"company\" SET \"active\" = true WHERE id IN ( SELECT \"c_0\".\"id\" FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"id\" = ? )"
                       :params (list (dt/number "1"))}]}
           (generate "company | where: id = 1 | update! active = true")))
    (is (= {:queries [{:table "company"
                       :query "UPDATE \"company\" SET \"deleted_at\" = NULL WHERE id IN ( SELECT \"c_0\".\"id\" FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"id\" = ? )"
                       :params (list (dt/number "1"))}]}
           (generate "company | where: id = 1 | update! deleted_at = null")))

    ;; Test JSONB type conversion
    (is (= {:queries [{:table "customer"
                       :query "UPDATE \"customer\" SET \"data\" = ?::jsonb WHERE id IN ( SELECT \"c_0\".\"id\" FROM \"customer\" AS \"c_0\" WHERE \"c_0\".\"id\" = ? )"
                       :params (list (dt/jsonb "{\"test\": 1}") (dt/number "1"))}]}
           (generate "customer | where: id = 1 | update! data = '{\"test\": 1}'")))

    ;; Test update with explicit table alias (disambiguates when multiple tables in context)
    (is (= {:queries [{:table "company"
                       :query "UPDATE \"company\" SET \"x\" = ? WHERE id IN ( SELECT \"c\".\"id\" FROM \"company\" AS \"c\" JOIN \"document\" AS \"d_1\" ON \"c\".\"id\" = \"d_1\".\"company_id\" )"
                       :params (list (dt/string "y"))}]}
           (generate "company as c | document | update! c.x = 'y'")))

    ;; Test multi-table update (runs multiple queries, one per table)
    (let [result (generate "company as c | w: id = 1 | document as d | w: type = 'invoice' | update! c.deleted_at = '2026-01-01', d.deleted_at = '2026-01-01'")]
      (is (= 2 (count (:queries result))))
      (is (= #{"company" "document"} (set (map :table (:queries result)))))))

  (testing "delete"
    (is (= {:query " /* No SQL. Evaluate the pine expression for results */ "}
           (generate "company | delete:")))))

(deftest test-action-operations
  (testing "Action operations should use different query execution path"
    ;; Test that update-action operations are identified correctly
    (let [state (-> "company | where: id = 1 | update! name = 'John'"
                    parser/parse
                    :result
                    (ast/generate :test))]
      (is (= :update-action (-> state :operation :type))))

    ;; Test that delete-action operations are identified correctly  
    (let [state (-> "company | delete! .id"
                    parser/parse
                    :result
                    (ast/generate :test))]
      (is (= :delete-action (-> state :operation :type))))))

(deftest test-format-query
  (testing "string"
    (is (= "\nSELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"name\" = 'Acme Inc.' LIMIT 250;\n"
           (-> "company | where: name='Acme Inc.'" generate eval/formatted-query))))

  (testing "Condition : date"
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"created_at\" = ?::timestamp LIMIT 250",
            :params (list (dt/date "2025-01-01"))}
           (generate "company | where: created_at = '2025-01-01'")))
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"created_at\" != ?::timestamp LIMIT 250",
            :params (list (dt/date "2025-01-01"))}
           (generate "company | where: created_at != '2025-01-01'")))
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"created_at\" > ?::timestamp LIMIT 250",
            :params (list (dt/date "2025-01-01"))}
           (generate "company | where: created_at > '2025-01-01'")))
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"created_at\" < ?::timestamp LIMIT 250",
            :params (list (dt/date "2025-01-01"))}
           (generate "company | where: created_at < '2025-01-01'"))))

  (testing "Casting placement - explicit vs automatic"
    ;; Test that explicit casts work on column side
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"customer\" AS \"c_0\" WHERE \"c_0\".\"uuid_col\"::uuid = ? LIMIT 250",
            :params (list (dt/uuid "1c50ee25-4938-4b77-b831-bc41a0ee3d0c"))}
           (generate "customer | where: uuid_col = '1c50ee25-4938-4b77-b831-bc41a0ee3d0c' ::uuid")))

    ;; Test that automatic casting works on value side without explicit cast
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"customer\" AS \"c_0\" WHERE \"c_0\".\"uuid_col\" = ?::uuid LIMIT 250",
            :params (list (dt/uuid "1c50ee25-4938-4b77-b831-bc41a0ee3d0c"))}
           (generate "customer | where: uuid_col = '1c50ee25-4938-4b77-b831-bc41a0ee3d0c'")))))

(testing "SQL generation with comments"
  (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" LIMIT 250",
          :params nil}
         (generate "-- select companies\ncompany")))
  (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"name\" = ? LIMIT 250",
          :params (map dt/string ["Acme"])}
         (generate "company /* get by name */ | where: name = 'Acme' -- exact match")))
  (is (= {:query "SELECT \"c_0\".\"id\", \"e_1\".\"name\", \"c_0\".id AS \"__c_0__id\", \"e_1\".id AS \"__e_1__id\" FROM \"company\" AS \"c_0\" JOIN \"employee\" AS \"e_1\" ON \"c_0\".\"id\" = \"e_1\".\"company_id\" LIMIT 250",
          :params nil}
         (generate "-- companies and employees\ncompany | s: id /* company id */ | employee | s: name -- employee name"))))

(deftest test-variables
  (testing "Single expression with |= produces normal SQL (assign is metadata)"
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" LIMIT 250"
            :params nil}
           (generate-expressions ["company |= active_companies"]))))

  (testing "Variable used as table generates CTE"
    (is (= {:query "WITH \"active_companies\" AS ( SELECT \"c_0\".* FROM \"company\" AS \"c_0\" ) SELECT \"active_companies\".* FROM \"active_companies\" AS \"active_companies\" LIMIT 250"
            :params nil}
           (generate-expressions ["company |= active_companies"
                                  "active_companies"]))))

  (testing "Variable with WHERE filter generates filtered CTE"
    (is (= {:query "WITH \"active_companies\" AS ( SELECT \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"name\" = ? ) SELECT \"active_companies\".* FROM \"active_companies\" AS \"active_companies\" LIMIT 250"
            :params (map dt/string ["Acme"])}
           (generate-expressions ["company | where: name = 'Acme' |= active_companies"
                                  "active_companies"]))))

  (testing "Join through a variable resolves correctly"
    (is (= {:query "WITH \"active_companies\" AS ( SELECT \"c_0\".* FROM \"company\" AS \"c_0\" ) SELECT \"e_1\".id AS \"__e_1__id\", \"e_1\".* FROM \"active_companies\" AS \"active_companies\" JOIN \"employee\" AS \"e_1\" ON \"active_companies\".\"id\" = \"e_1\".\"company_id\" LIMIT 250"
            :params nil}
           (generate-expressions ["company |= active_companies"
                                  "active_companies | employee"]))))

  (testing "Composed variables (variable of variable) generates flat CTEs"
    (is (= {:query "WITH \"active_companies\" AS ( SELECT \"c_0\".* FROM \"company\" AS \"c_0\" ), \"small_active\" AS ( SELECT \"active_companies\".* FROM \"active_companies\" AS \"active_companies\" LIMIT 10 ) SELECT \"small_active\".* FROM \"small_active\" AS \"small_active\" LIMIT 250"
            :params nil}
           (generate-expressions ["company |= active_companies"
                                  "active_companies | l: 10 |= small_active"
                                  "small_active"]))))

  (testing "Reverse join: child table navigates to variable wrapping its parent"
    ;; employee.company_id -> company.id
    ;; mytest = company (parent); employee | mytest should join employee to the CTE
    (is (= {:query "WITH \"mytest\" AS ( SELECT \"c_0\".* FROM \"company\" AS \"c_0\" ) SELECT \"e_0\".id AS \"__e_0__id\", \"mytest\".* FROM \"employee\" AS \"e_0\" JOIN \"mytest\" AS \"mytest\" ON \"e_0\".\"company_id\" = \"mytest\".\"id\" LIMIT 250"
            :params nil}
           (generate-expressions ["company |= mytest"
                                  "employee | mytest"]))))

  (testing "Reverse join: parent table navigates to variable wrapping its child"
    ;; employee.company_id -> company.id
    ;; mytest = employee (child); company | mytest should join company to the CTE
    (is (= {:query "WITH \"mytest\" AS ( SELECT \"e_0\".* FROM \"employee\" AS \"e_0\" ) SELECT \"c_0\".id AS \"__c_0__id\", \"mytest\".* FROM \"company\" AS \"c_0\" JOIN \"mytest\" AS \"mytest\" ON \"c_0\".\"id\" = \"mytest\".\"company_id\" LIMIT 250"
            :params nil}
           (generate-expressions ["employee |= mytest"
                                  "company | mytest"]))))

  (testing "Mid-pipeline assign: expression continues after |="
    ;; The assign snapshots the state at that point; subsequent ops still apply
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" LIMIT 10"
            :params nil}
           (generate-expressions ["company |= x | l: 10"])))
    ;; x is the unfiltered snapshot (no limit) — CTE body has no LIMIT
    (is (= {:query "WITH \"x\" AS ( SELECT \"c_0\".* FROM \"company\" AS \"c_0\" ) SELECT \"x\".* FROM \"x\" AS \"x\" LIMIT 250"
            :params nil}
           (generate-expressions ["company |= x | l: 10"
                                  "x"]))))

  (testing "Column reference via variable name resolves to the real SQL alias"
    ;; Within-expression: |= c still routes through pending-assignments → real table alias c_0
    (is (= {:query "SELECT \"e_1\".\"id\", \"c_0\".\"id\", \"c_0\".id AS \"__c_0__id\", \"e_1\".id AS \"__e_1__id\" FROM \"company\" AS \"c_0\" JOIN \"employee\" AS \"e_1\" ON \"c_0\".\"id\" = \"e_1\".\"company_id\" LIMIT 250"
            :params nil}
           (generate-expressions ["company |= c | employee | s: id, c.id"])))
    (is (true? (clojure.string/includes?
                (:query (generate-expressions ["company |= c | employee | w: c.id = 1"]))
                "WHERE \"c_0\".\"id\" = ?")))
    (is (true? (clojure.string/includes?
                (:query (generate-expressions ["company |= c | employee | o: c.id"]))
                "ORDER BY \"c_0\".\"id\"")))

    ;; Cross-expression: CTE alias = variable name, so x.id resolves to "x"."id"
    (is (true? (clojure.string/includes?
                (:query (generate-expressions ["company |= x"
                                               "x | w: x.id = 1"]))
                "WHERE \"x\".\"id\" = ?")))
    (is (true? (clojure.string/includes?
                (:query (generate-expressions ["company |= x"
                                               "x | o: x.id"]))
                "ORDER BY \"x\".\"id\""))))

  (testing "Same-source variables: two variables wrapping the same table join on id"
    (is (= {:query "WITH \"c1\" AS ( SELECT \"c_0\".* FROM \"company\" AS \"c_0\" ), \"c2\" AS ( SELECT \"c_0\".* FROM \"company\" AS \"c_0\" ) SELECT \"c2\".* FROM \"c1\" AS \"c1\" JOIN \"c2\" AS \"c2\" ON \"c1\".\"id\" = \"c2\".\"id\" LIMIT 250"
            :params nil}
           (generate-expressions ["company |= c1"
                                  "company |= c2"
                                  "c1 | c2"])))
    (is (= {:query "WITH \"c2\" AS ( SELECT \"c_0\".* FROM \"company\" AS \"c_0\" ), \"c1\" AS ( SELECT \"c_0\".* FROM \"company\" AS \"c_0\" ) SELECT \"c1\".* FROM \"c2\" AS \"c2\" JOIN \"c1\" AS \"c1\" ON \"c2\".\"id\" = \"c1\".\"id\" LIMIT 250"
            :params nil}
           (generate-expressions ["company |= c1"
                                  "company |= c2"
                                  "c2 | c1"])))))

(deftest test-checkpoints
  (testing "LIMIT checkpoint: auto-CTE when a table op follows limit"
    (is (= {:query "WITH \"__pine_0__\" AS ( SELECT \"c_0\".* FROM \"x\".\"company\" AS \"c_0\" LIMIT 10 ) SELECT \"e_1\".id AS \"__e_1__id\", \"e_1\".* FROM \"__pine_0__\" AS \"__pine_0__\" JOIN \"employee\" AS \"e_1\" ON \"__pine_0__\".\"id\" = \"e_1\".\"company_id\" LIMIT 250"
            :params nil}
           (generate "x.company | l: 10 | employee"))))

  (testing "LIMIT checkpoint with explicit user-named CTE via |="
    (is (= {:query "WITH \"pg\" AS ( SELECT \"c_0\".* FROM \"x\".\"company\" AS \"c_0\" LIMIT 10 ) SELECT \"e_1\".id AS \"__e_1__id\", \"e_1\".* FROM \"pg\" AS \"pg\" JOIN \"employee\" AS \"e_1\" ON \"pg\".\"id\" = \"e_1\".\"company_id\" LIMIT 250"
            :params nil}
           (generate "x.company | l: 10 |= pg | employee"))))

  (testing "GROUP checkpoint: auto-CTE when a table op follows group"
    (is (= {:query "WITH \"__pine_0__\" AS ( SELECT \"c_0\".\"id\", COUNT(1) AS \"count\" FROM \"x\".\"company\" AS \"c_0\" GROUP BY \"c_0\".\"id\" ) SELECT \"e_1\".id AS \"__e_1__id\", \"e_1\".* FROM \"__pine_0__\" AS \"__pine_0__\" JOIN \"employee\" AS \"e_1\" ON \"__pine_0__\".\"id\" = \"e_1\".\"company_id\" LIMIT 250"
            :params nil}
           (generate "x.company | group: id => count | employee"))))

  (testing "Checkpoint does not fire for non-table ops after limit"
    ;; LIMIT followed by COUNT: checkpoint holds, COUNT builds its own wrapper CTE
    (is (= {:query "WITH x AS ( SELECT \"c_0\".* FROM \"company\" AS \"c_0\" LIMIT 100 ) SELECT COUNT(*) FROM x"
            :params nil}
           (generate "company | limit: 100 | count:"))))

  (testing "Standalone GROUP without following table still uses build-group-query"
    ;; Existing behaviour must not regress: the GROUP dispatch path is unaffected
    (is (clojure.string/includes?
         (:query (generate "x.company | group: id => count"))
         "GROUP BY"))))