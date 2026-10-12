(ns pine.sealed-aliases-test
  "A column written with the alias of a table a checkpoint sealed. The
  checkpoint's CTE is the only table the pipe is on afterwards, so the
  column is read as the CTE's column that kept it. See
  docs/checkpoints.md."
  (:require [clojure.string :as s]
            [clojure.test :refer [deftest is testing]]
            [pine.ast.main :as ast]
            [pine.eval :as eval]
            [pine.parser :as parser]))

(defn- sql
  ([expression] (sql :test expression))
  ([connection expression]
   (-> expression parser/parse :result (ast/generate connection nil nil {} []) eval/build-query eval/formatted-query s/trim)))

(defn- error [expression]
  (try (sql expression) nil
       (catch clojure.lang.ExceptionInfo e (.getMessage e))))

(defn- outer
  "The query after the last CTE: what the steps after the checkpoint built."
  [query]
  (subs query (s/last-index-of query " ) SELECT ")))

(deftest test-a-kept-column
  (testing "After group:, a grouped column of a sealed table is the CTE's column"
    (is (= " ) SELECT \"result_1\".\"id\" FROM \"result_1\" AS \"result_1\" LIMIT 250;"
           (outer (sql "company as c | employee .company_id | group: c.id | s: c.id")))))

  (testing "Under its `as` name, when it was selected with one"
    (is (= " ) SELECT \"result_1\".\"cid\", \"result_1\".\"name\" FROM \"result_1\" AS \"result_1\" LIMIT 250;"
           (outer (sql "company as c | employee .company_id | s: c.id as cid, e_1.name | o: created_at desc | l: 1 per c.id | s: c.id, name")))))

  (testing "The current table of a plain limit: is kept whole"
    (is (= " ) SELECT \"result_1\".* FROM \"result_1\" AS \"result_1\" ORDER BY \"result_1\".\"id\" DESC LIMIT 250;"
           (outer (sql "company as c | employee .company_id | l: 10 | o: e_1.id")))))

  (testing "In where:, on either side of a comparison"
    (is (s/ends-with? (sql "company as c | employee .company_id | o: created_at desc | l: 1 per c.id | where: e_1.id = 1")
                      "WHERE \"result_1\".\"id\" = '1' LIMIT 250;"))
    (is (s/ends-with? (sql "company as c | employee .company_id | s: c.id as cid, e_1.company_id | l: 10 | where: company_id = c.id")
                      "WHERE \"result_1\".\"company_id\" = \"result_1\".\"cid\" LIMIT 250;")))

  (testing "Through two checkpoints"
    (is (= " ) SELECT \"result_2\".\"id\" FROM \"result_2\" AS \"result_2\" LIMIT 250;"
           (outer (sql "company as c | employee .company_id | group: c.id | l: 5 | s: c.id"))))))

(deftest test-dialects
  (testing "The same reading on MySQL and SQLite"
    (is (= " ) SELECT `result_1`.`cid`, `result_1`.`name` FROM `result_1` AS `result_1` LIMIT 250;"
           (outer (sql :test-mysql "company as c | employee .company_id | s: c.id as cid, e_1.name | o: created_at desc | l: 1 per c.id | s: c.id, name"))))
    (is (= " ) SELECT \"result_1\".\"cid\", \"result_1\".\"name\" FROM \"result_1\" AS \"result_1\" LIMIT 250;"
           (outer (sql :test-sqlite "company as c | employee .company_id | s: c.id as cid, e_1.name | o: created_at desc | l: 1 per c.id | s: c.id, name"))))
    (is (s/ends-with? (sql :test-mysql "company as c | employee .company_id | group: c.id | where: c.id = 1")
                      "WHERE `result_1`.`id` = '1' LIMIT 250;"))
    (is (s/ends-with? (sql :test-sqlite "company as c | employee .company_id | l: 10 | o: e_1.id")
                      "ORDER BY \"result_1\".\"id\" DESC LIMIT 250;"))))

(deftest test-refused
  (testing "A column the checkpoint didn't keep is an error naming what it keeps"
    (is (= (str "`c.id` isn't a column of `result_1`. The limit: before sealed `c` into it, and it keeps id, company_id, reports_to. "
                "To use `c.id` after the limit:, select it before: `s: c.id as c_id, ...`.")
           (error "company as c | employee .company_id | o: created_at desc | l: 1 per c.id | s: c.id")))
    (is (s/starts-with? (error "company as c | employee .company_id | l: 10 | where: company_id = c.id")
                        "`c.id` isn't a column of `result_1`. The limit: before"))
    (is (s/starts-with? (error "company as c | employee .company_id | group: e_1.name | o: c.id")
                        "`c.id` isn't a column of `result_1`. The group: before")))

  (testing "A name that is two columns of the checkpoint can't pick one"
    (let [message "`result_1` has two columns named `id`, so `%s` can't pick one. Name them apart with `as` before the limit:, like `s: c.id as c_id, id`."]
      (is (= (format message "id")
             (error "company as c | employee .company_id | s: c.id, id | o: created_at desc | l: 1 per c.id | s: id")))
      (is (= (format message "c.id")
             (error "company as c | employee .company_id | s: c.id, id | l: 10 | where: c.id = 1"))))))

(deftest test-unchanged
  (testing "A table piped after the checkpoint is not sealed, even with a sealed table's alias"
    (is (s/ends-with? (sql "company as c | l: 10 | employee as c2 .company_id | s: c2.id")
                      "SELECT \"c2\".\"id\", \"c2\".\"id\" AS \"__c2__id\" FROM \"result_1\" AS \"result_1\" JOIN \"employee\" AS \"c2\" ON \"result_1\".\"id\" = \"c2\".\"company_id\" LIMIT 250;")))

  (testing "No checkpoint, no change"
    (is (= "SELECT \"c\".\"id\", \"c\".\"id\" AS \"__c__id\", \"e_1\".\"id\" AS \"__e_1__id\" FROM \"company\" AS \"c\" JOIN \"employee\" AS \"e_1\" ON \"c\".\"id\" = \"e_1\".\"company_id\" LIMIT 250;"
           (sql "company as c | employee .company_id | s: c.id")))))
