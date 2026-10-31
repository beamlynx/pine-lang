(ns pine.db.joins-test
  "Joins on real SQLite, MySQL and Postgres databases. See pine.db.engines."
  (:require [clojure.test :refer [deftest is testing]]
            [pine.db.engines :as e]))

(def ^:private fixture
  ["CREATE TABLE company (id INTEGER PRIMARY KEY, name {text})"
   "CREATE TABLE resource (id INTEGER PRIMARY KEY, kind {text}, ref_id {text})"
   "CREATE TABLE doc (id INTEGER PRIMARY KEY, name {text}, data {json})"
   "INSERT INTO company VALUES (1, 'Acme'), (2, 'Globex'), (3, 'Initech')"
   "INSERT INTO resource VALUES (1, 'Company', '1'), (2, 'Company', '3'), (3, 'Person', '2')"
   (str "INSERT INTO doc VALUES "
        "(1, 'Ann', '{\"companyIds\": [1, 3], \"companies\": [{\"id\": 2}]}'), "
        "(2, 'Bob', '{\"companyIds\": [2], \"companies\": []}')")])

(deftest test-join-from-a-name
  (e/on-every-engine
   fixture
   (fn [db]
     (testing "a column named `company_id` with `as` joins `company` on its id, even of another type"
       ;; ref_id is text and company.id an integer, so both are compared as text.
       (is (= #{[1 "1" "Acme"] [2 "3" "Initech"]}
              (set (e/rows db "resource | where: kind = 'Company' | s: id, ref_id as company_id | company | s: name")))))

     (testing "without `as`, nothing joins them"
       (is (re-find #"No relation" (str (e/refused db "resource | s: ref_id | company")))))

     (testing "a key inside JSON named with `as`"
       (is (= [["1" "Acme"]]
              (mapv (fn [[ref company]] [(str ref) company])
                    (e/rows db "doc | where: id = 1 | s: data.companyIds[0] as company_id | company | s: name")))))

     (testing "elements named `company_id` join `company` on each element"
       (is (= #{["Ann" "Acme"] ["Ann" "Initech"] ["Bob" "Globex"]}
              (set (e/rows db "doc | unnest: data.companyIds as company_id | company | s: d_0.name, name")))))

     (testing "elements named after the table join it on their `id`"
       (is (= #{["Ann" "Globex"]}
              (set (e/rows db "doc | unnest: data.companies as company | company | s: d_0.name, name"))))))))
