(ns pine.db.queries-test
  "Reading data, on real SQLite, MySQL and Postgres databases: joins,
  conditions, grouping, checkpoints and computed values. See
  pine.db.engines."
  (:require [clojure.test :refer [deftest is testing]]
            [pine.db.engines :as e]))

(def ^:private fixture
  ["CREATE TABLE company (id INTEGER PRIMARY KEY, name {text} NOT NULL)"
   (str "CREATE TABLE person (id INTEGER PRIMARY KEY, name {text}, company_id INTEGER, active {bool}, "
        "FOREIGN KEY (company_id) REFERENCES company (id))")
   "CREATE TABLE doc (id INTEGER PRIMARY KEY, name {text}, data {json})"
   "INSERT INTO company VALUES (1, 'Acme'), (2, 'Globex')"
   "INSERT INTO person VALUES (1, 'Ann', 1, TRUE), (2, 'Bob', 1, FALSE), (3, 'Cy', 2, TRUE)"
   (str "INSERT INTO doc VALUES "
        "(1, 'Ann', '{\"country\": \"SE\"}'), (2, 'Bob', '{\"country\": \"NO\"}'), "
        "(3, 'Cy', '{\"country\": null}'), (4, 'Di', NULL)")])

(defn- truthy
  "A condition's value as 1 or 0: Postgres returns true and false, MySQL and
  SQLite 1 and 0."
  [v]
  (if (contains? #{true 1} v) 1 0))

(deftest test-queries
  (e/on-every-engine
   fixture
   (fn [db]
     (testing "a foreign-key join"
       (is (= [["Ann" "Acme"] ["Bob" "Acme"] ["Cy" "Globex"]]
              (e/rows db "person as p | company as c | select: p.name, c.name | order: p.id asc"))))

     (testing "ilike matches whatever the case"
       (is (= ["Ann"] (e/column db "person | where: name ilike 'a%' | select: name"))))

     (testing "a boolean column"
       (is (= ["Ann" "Cy"] (e/column db "person | where: active = true | select: name | order: id asc"))))

     (testing "group and count"
       (is (= [[1 2] [2 1]] (e/rows db "person | select: company_id | group: company_id with count | order: company_id asc")))
       (is (= [[3]] (e/rows db "person | count:")))
       (is (= [[3]] (e/rows db "person | group: with count as n")))
       (is (= [[2 4]] (mapv #(mapv long %) (e/rows db "person | where: active = true | group: with count as n, id => sum as ids")))))

     (testing "a sealed table's column after a checkpoint is the checkpoint's column"
       (is (= [1 2] (e/column db "company as c | person .company_id | group: c.id | s: c.id | o: id asc")))
       (is (= [[1 "Bob"] [2 "Cy"]]
              (mapv #(vec (take 2 %))
                    (e/rows db "company as c | person .company_id | s: c.id as cid, p_1.name | o: name desc | l: 1 per c.id | s: c.id, name | o: cid asc"))))))))

(deftest test-computed-values
  (e/on-every-engine
   fixture
   (fn [db]
     (testing "coalesce, and concat is NULL when any part is"
       (is (= ["SE" "NO" "none" "none"] (e/column db "doc | s: coalesce(data.country, 'none') as c | o: id asc")))
       (is (= ["n:SE" "n:NO" nil nil] (e/column db "doc | s: concat('n:', data.country) as c | o: id asc"))))

     (testing "if, and a condition as a value"
       (is (= ["on" "off" "on"] (e/column db "person | s: if active = true then 'on' else 'off' as s | o: id asc")))
       (is (= [1 0 0] (mapv truthy (e/column db "person | s: name like 'A%' as a | o: id asc")))))

     (testing "its name in where:, order: and after per"
       (is (= ["Cy"] (e/column db "person | s: name, if active = true then 'on' else 'off' as s | where: s = 'on' | o: name desc | l: 1")))
       (is (= ["Bob" "Cy"] (e/column db "person | s: name, company_id, coalesce(company_id, 0) as c | o: name desc | l: 1 per c | o: name asc"))))

     (testing "aggregates with a filter, any and all"
       (is (= [[1 1 1 0 1] [2 3 1 1 1]]
              (mapv (fn [[k first-id some-on all-on n]] [k first-id (truthy some-on) (truthy all-on) n])
                    (e/rows db "person | group: company_id with min(id) as first, any(active = true) as some_on, all(active = true) as all_on, count where active = true as n | o: company_id asc"))))))))

(deftest test-json-paths
  (e/on-every-engine
   ["CREATE TABLE doc (id INTEGER PRIMARY KEY, name {text}, data {json})"
    (str "INSERT INTO doc VALUES "
         "(1, 'Ann', '{\"country\": \"SE\", \"age\": 31, \"vip\": true, \"address\": {\"city\": \"Oslo\"}, \"tags\": [\"a\", \"b\"], \"home address\": \"x\"}'), "
         "(2, 'Bob', '{\"country\": \"NO\", \"age\": \"31\", \"vip\": false, \"address\": {\"city\": \"Bergen\"}}'), "
         "(3, 'Cy', '{\"country\": null, \"age\": 20}'), "
         "(4, 'Di', NULL)")]
   (fn [db]
     (testing "= compares typed JSON values: a string \"31\" is not the number 31"
       (is (= ["Ann"] (e/column db "doc | where: data.country = 'SE' | select: name")))
       (is (= ["Ann"] (e/column db "doc | where: data.age = 31 | select: name")))
       (is (= ["Ann"] (e/column db "doc | where: data.vip = true | select: name"))))

     (testing "< and > skip a value that isn't a number"
       (is (= ["Ann"] (e/column db "doc | where: data.age > 30 | select: name")))
       (is (= ["Cy"] (e/column db "doc | where: data.age < 30 | select: name"))))

     (testing "like and in compare text; is null is true for a JSON null and for a missing key"
       (is (= ["Ann"] (e/column db "doc | where: data.country like 'S%' | select: name")))
       (is (= ["Ann" "Bob"] (e/column db "doc | where: data.country in ('SE', 'NO') | select: name | order: id asc")))
       (is (= ["Cy" "Di"] (e/column db "doc | where: data.country is null | select: name | order: id asc"))))

     (testing "nested keys, indexes and quoted keys"
       (is (= [["Ann" "Oslo" "a"] ["Bob" "Bergen" nil] ["Cy" nil nil] ["Di" nil nil]]
              (e/rows db "doc | select: name, data.address.city, data.tags[0] | order: id asc")))
       (is (= ["x" nil nil nil] (e/column db "doc | select: data.'home address' | order: id asc"))))

     (testing "group by a key"
       (is (= #{[nil 2] ["NO" 1] ["SE" 1]}
              (set (e/rows db "doc | select: data.country | group: data.country with count"))))))))
