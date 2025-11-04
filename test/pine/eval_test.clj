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
            :params []}
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
            :params (map dt/string [])}
           (generate "company | where: name = country")))
    (is (= {:query "SELECT \"c\".id AS \"__c__id\", \"c\".* FROM \"company\" AS \"c\" WHERE \"c\".\"name\" != \"c\".\"country\" LIMIT 250",
            :params (map dt/string [])}
           (generate "company as c | name != c.country")))
    (is (= {:query "SELECT \"c\".id AS \"__c__id\", \"c\".* FROM \"company\" AS \"c\" WHERE \"c\".\"name\" != \"c\".\"country\" LIMIT 250",
            :params (map dt/string [])}
           (generate "company as c | c.name != c.country"))))

  (testing "Condition : NULL"
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"deleted_at\" IS NULL LIMIT 250",
            :params []}
           (generate "company | where: deleted_at is null")))
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"deleted_at\" IS NOT NULL LIMIT 250",
            :params []}
           (generate "company | where: deleted_at is not null")))
    (is (= {:query "SELECT \"c_0\".id AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"deleted_at\" IS NULL LIMIT 250",
            :params []}
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
           (generate "x.company | y.employee :left | z.document :right"))))

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
    (is (= {:query "SELECT \"e_0\".\"status\", COUNT(1) FROM \"email\" AS \"e_0\" GROUP BY \"e_0\".\"status\"",
            :params nil}
           (generate "email | group: status => count"))))

  (testing "date extraction functions"
    ;; Year extraction
    (is (= {:query "SELECT DATE_TRUNC('year', \"e_0\".\"created_at\")::date AS \"year\", \"e_0\".id AS \"__e_0__id\" FROM \"employee\" AS \"e_0\" LIMIT 250",
            :params nil}
           (generate "employee | select: created_at => year")))

    ;; Month extraction
    (is (= {:query "SELECT DATE_TRUNC('month', \"e_0\".\"created_at\")::date AS \"month\", \"e_0\".id AS \"__e_0__id\" FROM \"employee\" AS \"e_0\" LIMIT 250",
            :params nil}
           (generate "employee | select: created_at => month")))

    ;; Day extraction
    (is (= {:query "SELECT DATE_TRUNC('day', \"e_0\".\"created_at\")::date AS \"day\", \"e_0\".id AS \"__e_0__id\" FROM \"employee\" AS \"e_0\" LIMIT 250",
            :params nil}
           (generate "employee | select: created_at => day")))

    ;; Hour extraction (uses timestamp)
    (is (= {:query "SELECT DATE_TRUNC('hour', \"e_0\".\"created_at\")::timestamp AS \"hour\", \"e_0\".id AS \"__e_0__id\" FROM \"employee\" AS \"e_0\" LIMIT 250",
            :params nil}
           (generate "employee | select: created_at => hour")))

    ;; With alias
    (is (= {:query "SELECT DATE_TRUNC('month', \"e\".\"created_at\")::date AS \"month\", \"e\".id AS \"__e__id\" FROM \"employee\" AS \"e\" LIMIT 250",
            :params nil}
           (generate "employee as e | select: e.created_at => month")))

    ;; Mixed with regular columns
    (is (= {:query "SELECT \"e_0\".\"name\", DATE_TRUNC('year', \"e_0\".\"created_at\")::date AS \"year\", \"e_0\".id AS \"__e_0__id\" FROM \"employee\" AS \"e_0\" LIMIT 250",
            :params nil}
           (generate "employee | select: name, created_at => year"))))

  (testing "date extraction with grouping"
    ;; Group by name with month extraction
    (is (= {:query "SELECT \"e_0\".\"name\", DATE_TRUNC('month', \"e_0\".\"created_at\")::date AS \"month\", COUNT(1) FROM \"employee\" AS \"e_0\" GROUP BY \"e_0\".\"name\", \"month\"",
            :params nil}
           (generate "employee | select: name, created_at => month | group: name, created_at => count")))

    ;; Group by just the extracted date
    (is (= {:query "SELECT DATE_TRUNC('month', \"e_0\".\"created_at\")::date AS \"month\", COUNT(1) FROM \"employee\" AS \"e_0\" GROUP BY \"month\"",
            :params nil}
           (generate "employee | select: created_at => month | group: created_at => count"))))

  (testing "delete action"
    (is (= {:query "DELETE FROM \"company\" WHERE \"id\" IN ( SELECT \"c_0\".\"id\" FROM \"company\" AS \"c_0\" )",
            :params nil}
           (generate "company | delete! .id"))))

  (testing "update action"
    (is (= {:query "UPDATE \"company\" SET \"name\" = ? WHERE id IN ( SELECT \"c_0\".\"id\" FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"id\" = ? )",
            :params (list (dt/string "John Doe") (dt/number "1"))}
           (generate "company | where: id = 1 | update! name = 'John Doe'")))
    (is (= {:query "UPDATE \"company\" SET \"name\" = ?, \"age\" = ? WHERE id IN ( SELECT \"c_0\".\"id\" FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"id\" = ? )",
            :params (list (dt/string "John") (dt/number "30") (dt/number "1"))}
           (generate "company | where: id = 1 | update! name = 'John', age = 30")))
    (is (= {:query "UPDATE \"company\" SET \"active\" = true WHERE id IN ( SELECT \"c_0\".\"id\" FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"id\" = ? )",
            :params (list (dt/number "1"))}
           (generate "company | where: id = 1 | update! active = true")))

    ;; Test JSONB type conversion
    (is (= {:query "UPDATE \"customer\" SET \"data\" = ?::jsonb WHERE id IN ( SELECT \"c_0\".\"id\" FROM \"customer\" AS \"c_0\" WHERE \"c_0\".\"id\" = ? )",
            :params (list (dt/jsonb "{\"test\": 1}") (dt/number "1"))}
           (generate "customer | where: id = 1 | update! data = '{\"test\": 1}'"))))

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