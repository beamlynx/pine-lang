(ns pine.db.unnest-test
  "`unnest:` on real SQLite, MySQL and Postgres databases: the same
  expressions, and the same rows, on each. The SQL each one writes is tested
  as text in pine.json-paths-test. See pine.db.engines."
  (:require [clojure.test :refer [deftest is testing]]
            [pine.api :as api]
            [pine.db.engines :as e]))

(def ^:private fixture
  ["CREATE TABLE company (id INTEGER PRIMARY KEY, name {text})"
   "CREATE TABLE doc (id INTEGER PRIMARY KEY, name {text}, data {json})"
   "CREATE TABLE note (id INTEGER PRIMARY KEY, body {text})"
   "INSERT INTO company VALUES (1, 'Acme'), (2, 'Globex'), (3, 'Initech')"
   (str "INSERT INTO doc VALUES "
        "(1, 'Ann', '{\"tags\": [\"a\", \"b\"], \"items\": [{\"sku\": \"x\", \"qty\": 1, \"parts\": [\"p1\", \"p2\"]}, {\"sku\": \"y\", \"qty\": 2, \"parts\": []}], \"companyIds\": [1, 3], \"companies\": [{\"id\": 2}]}'), "
        "(2, 'Bob', '{\"tags\": [\"b\", null], \"items\": [{\"sku\": \"x\", \"qty\": 5, \"parts\": [\"p3\"]}], \"companyIds\": [2], \"companies\": []}'), "
        "(3, 'Cy', '{\"tags\": \"not an array\"}'), "
        "(4, 'Di', NULL)")
   "INSERT INTO note VALUES (1, '[\"x\", \"y\"]')"])

(def ^:private tables {"company" [:id] "doc" [:id] "note" [:id]})

(deftest test-unnest
  (e/on-every-engine
   fixture
   (fn [{:keys [engine] :as db}]
     (testing "one row for each element, next to the row it came from; a JSON null element is NULL"
       (is (= #{["Ann" "a"] ["Ann" "b"] ["Bob" "b"] ["Bob" nil]}
              (set (e/rows db "doc | unnest: data.tags as t | s: name, t")))))

     (testing "a row whose value isn't an array, or is null, has no elements"
       (is (= #{["Ann"] ["Bob"]} (set (e/rows db "doc | unnest: data.tags as t | s: name")))))

     (testing "keys of an element, and a condition on one"
       ;; A selected key is text on MySQL and Postgres, and SQLite's own
       ;; value (here a number), so it is compared as text.
       (is (= #{["Ann" "1"] ["Bob" "5"]}
              (set (map (fn [[n q]] [n (str q)])
                        (e/rows db "doc | unnest: data.items as i | where: i.sku = 'x' | s: name, i.qty"))))))

     (testing "elements of elements"
       (is (= #{["Ann" "p1"] ["Ann" "p2"] ["Bob" "p3"]}
              (set (e/rows db "doc | unnest: data.items as i | unnest: i.parts as p | s: name, p")))))

     (testing "grouped by an element"
       (is (= #{["a" 1] ["b" 2] [nil 1]} (set (e/rows db "doc | unnest: data.tags as t | group: t with count")))))

     (testing "the pipe stays on the table: a bare name is its column"
       (is (= [[2]] (e/rows db "doc | unnest: data.tags as t | where: t = 'b' | where: name = 'Bob' | s: id"))))

     (testing "on SQLite, which keeps JSON in text, a text column can be unnested, and is suggested"
       (if (= engine :sqlite)
         (do (is (= #{["x"] ["y"]} (set (e/rows db "note | unnest: body as b | s: b"))))
             (is (= ["body"] (map :column (get-in (api/api-build "note | unnest: " nil (:id db)) [:ast :hints :unnest])))))
         (is (re-find #"not a JSON column" (str (e/refused db "note | unnest: body as b")))))))))

(deftest test-unnest-writes
  (e/on-every-engine
   fixture
   (fn [db]
     (testing "update! after unnest: changes the table's rows that have a matching element, once each"
       (let [before (e/snapshot db tables)]
         (is (= [["doc" 2]] (e/write! db "doc | unnest: data.tags as t | where: t = 'b' | update! name = 'Tagged'")))
         (is (= {"doc" {:changed {[1] {:name ["Ann" "Tagged"]} [2] {:name ["Bob" "Tagged"]}}}}
                (e/changes before (e/snapshot db tables))))))

     (testing "delete! after unnest: deletes them"
       (let [before (e/snapshot db tables)]
         (e/write! db "doc | unnest: data.items as i | where: i.qty = 5 | delete! .id")
         (is (= {"doc" {:deleted #{[2]}}} (e/changes before (e/snapshot db tables)))))))))
