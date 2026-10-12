(ns pine.sql.hints-test
  (:require [clojure.test :refer [deftest is testing]]
            [pine.api :as api]
            [pine.db.main :as db]
            [pine.db.references :as refs]
            [pine.sql.hints :as h]))

(defn- joins
  ([sql] (joins sql (count sql)))
  ([sql cursor]
   (mapv :sql (:joins (h/join-hints {:query sql
                                     :cursor cursor
                                     :dialect :postgres
                                     :references (db/init-references :test)
                                     :relations #(get-in (api/api-build [%] nil :test) [:ast :hints :table])})))))

(deftest test-join-hints
  (testing "The tables that join to the query's, with the ON clause written, in the SQL's aliases"
    (is (= ["y.employee e ON e.company_id = c.id" "z.document d ON d.company_id = c.id"]
           (joins "select * from company c join "))))

  (testing "Both directions, the self join, and an alias that is taken gets a number"
    (is (= ["x.company c ON c.id = e.company_id"
            "y.employee e2 ON e2.id = e.reports_to"
            "z.document d ON d.employee_id = e.id"
            "z.document d ON d.created_by = e.id"
            "y.employee e2 ON e2.reports_to = e.id"]
           (joins "select * from employee e left join comp"))))

  (testing "Every table in the query, the last one first"
    (is (= "y.employee e2 ON e2.company_id = c.id"
           (nth (joins "select * from x.company c join y.employee e on e.company_id = c.id join ") 5))))

  (testing "A table without an alias is referred to by its name"
    (is (= "y.employee e ON e.company_id = company.id" (first (joins "select * from company join ")))))

  (testing "A reserved word is quoted, and the default schema left out"
    (is (= ["\"order\" o ON o.user_id = u.id"] (joins "select * from \"user\" u join "))))

  (testing "Only after JOIN, and not inside a string or comment"
    (is (= [] (joins "select * from company c where ")))
    (is (= [] (joins "select * from company c where c.name = 'x join ")))
    (is (= [] (joins "select * from company c -- join "))))

  (testing "Only the text before the cursor"
    (is (= 2 (count (joins "select * from company c join  where c.id = 1" 29))))))

(deftest test-names
  (let [references (refs/index-references [[] [["public" "Invoice" "id" 1 "text" nil "YES" nil]
                                               ["public" "lineItem" "invoiceId" 1 "text" nil "YES" nil]] []])
        hint {:schema "public" :table "lineItem" :column "invoiceId" :related-column "id" :resolution "heuristic"}
        joins (fn [dialect sql]
                (mapv :sql (:joins (h/join-hints {:query sql :cursor (count sql) :dialect dialect
                                                  :references references
                                                  :relations (fn [pine] (when (= "Invoice | " pine) [hint]))}))))]
    (testing "A bare name in any case finds the table, and a mixed-case name is quoted"
      (is (= ["\"lineItem\" li ON li.\"invoiceId\" = i.id"] (joins :postgres "select * from invoice i join "))))
    (testing "In MySQL, with backquotes only where needed"
      (is (= ["lineItem li ON li.invoiceId = i.id"] (joins :mysql "select * from invoice i join "))))))
