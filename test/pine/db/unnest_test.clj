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

(defn- hidden
  "Each row's value in column 0 and in the hidden column `column-alias`."
  [db expression column-alias]
  (let [{:keys [header rows error]} (e/pine db expression)
        i (.indexOf ^java.util.List header column-alias)]
    (is (nil? error) (str expression ": " error))
    (is (<= 0 i) (str column-alias " in " header))
    (set (map (fn [row] [(first row) (nth row i)]) rows))))

(deftest test-element-positions
  (e/on-every-engine
   fixture
   (fn [db]
     (testing "each element's position in its array, from 0"
       (is (= #{["a" 0] ["b" 1]}
              (set (map (fn [[t i]] [t (long i)]) (hidden db "doc | where: id = 1 | unnest: data.tags as t | s: t" "__t__index")))))
       (is (= #{["p1" 0] ["p2" 1] ["p3" 0]}
              (set (map (fn [[p i]] [p (long i)]) (hidden db "doc | unnest: data.items as i | unnest: i.parts as p | s: p" "__p__index"))))))

     (testing "and the JSON type of each value the grid can edit"
       (is (= #{["1" "number"] ["2" "number"] ["5" "number"]}
              (set (map (fn [[q t]] [(str q) t]) (hidden db "doc | unnest: data.items as i | s: i.qty" "__i__qty__type"))))))

     (testing "`e` by itself is labelled as the element, not as a column of the table"
       (is (= {:alias "t" :column "value" :path [] :column-alias "t"}
              (-> (e/pine db "doc | where: id = 2 | unnest: data.tags as t") :columns last
                  (select-keys [:alias :column :path :column-alias]))))))))

(defn- data-change
  "Runs the update the results grid writes for one element value, and
  returns what changed in every table: only doc `id`'s data may."
  [db expression]
  (let [before (e/snapshot db tables)]
    (e/write! db expression)
    (e/changes before (e/snapshot db tables))))

(deftest test-element-edits
  (e/on-every-engine
   fixture
   (fn [db]
     (let [ann-data (fn [] (get-in (e/table db "doc" [:id]) [[1] :data]))]
       (testing "a key of one element: that element's key changes, and nothing else"
         (let [before (ann-data)
               changes (data-change db "doc | unnest: data.items as i | from: d_0 | where: id = 1 | update! d_0.data.items[1].qty = 7")]
           (is (= #{"doc"} (set (keys changes))))
           (is (= [[1]] (keys (get-in changes ["doc" :changed]))))
           (is (= (assoc-in before ["items" 1 "qty"] 7) (ann-data)))
           (is (= "number" (e/json-type db "doc" "data" "id" 1 ["items" 1 "qty"])) "and a number stays a number")))

       (testing "an element that is a value by itself"
         (let [before (ann-data)]
           (data-change db "doc | unnest: data.tags as t | from: d_0 | where: id = 1 | update! d_0.data.tags[0] = 'A'")
           (is (= (assoc-in before ["tags" 0] "A") (ann-data)))))

       (testing "an element of an element: a position at each level"
         (let [before (ann-data)]
           (data-change db "doc | unnest: data.items as i | unnest: i.parts as p | from: d_0 | where: id = 1 | update! d_0.data.items[0].parts[1] = 'P2'")
           (is (= (assoc-in before ["items" 0 "parts" 1] "P2") (ann-data)))))))))

(deftest test-unnest-left
  (e/on-every-engine
   fixture
   (fn [db]
     (testing "`:left` keeps a row with no elements, once, with the element empty"
       (is (= #{["Ann" "a"] ["Ann" "b"] ["Bob" "b"] ["Bob" nil] ["Cy" nil] ["Di" nil]}
              (set (e/rows db "doc | unnest: data.tags as t :left | s: name, t")))))

     (testing "a value that is an object, not an array, keeps its row too"
       (is (= #{["Ann" nil] ["Bob" nil] ["Cy" nil] ["Di" nil]}
              (set (e/rows db "doc | unnest: data as d :left | s: name, d")))))

     (testing "so one unnest: of a value that isn't an array no longer empties the result"
       (is (= #{} (set (e/rows db "doc | unnest: data.tags as t | unnest: data as d | s: name, t"))) "without :left")
       (is (= #{["Ann" "a"] ["Ann" "b"] ["Bob" "b"] ["Bob" nil]}
              (set (e/rows db "doc | unnest: data.tags as t | unnest: data as d :left | s: name, t")))))

     (testing "elements of elements, each :left on its own"
       (is (= #{["Ann" "x" "p1"] ["Ann" "x" "p2"] ["Ann" "y" nil] ["Bob" "x" "p3"]}
              (set (e/rows db "doc | unnest: data.items as i | unnest: i.parts as p :left | s: name, i.sku, p")))))

     (testing "a row without elements has no position, so the grid can't edit an element there"
       (is (= #{["a" 0] ["b" 1] ["b" 0] [nil 1] [nil nil]}
              (set (map (fn [[t i]] [t (some-> i long)])
                        (hidden db "doc | unnest: data.tags as t :left | s: t" "__t__index"))))))

     (testing "update! after :left changes the rows the condition selects, those without elements too"
       (let [before (e/snapshot db tables)]
         (e/write! db "doc | unnest: data.tags as t :left | where: t is null | update! name = 'none'")
         (is (= {"doc" {:changed {[2] {:name ["Bob" "none"]} [3] {:name ["Cy" "none"]} [4] {:name ["Di" "none"]}}}}
                (e/changes before (e/snapshot db tables)))))))))
