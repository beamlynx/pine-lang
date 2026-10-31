(ns pine.db.writes-test
  "update! and delete! on real SQLite, MySQL and Postgres databases.

  Each scenario runs on a fresh database. It reads every row of every table
  before and after the write, with plain SQL rather than Pine, and asserts
  the whole difference: what had to change, and that nothing else did. A
  write that changes one row too many fails here. See pine.db.engines."
  (:require [clojure.string :as s]
            [clojure.test :refer [deftest is testing]]
            [pine.db.engines :as e]))

(def ^:private fixture
  ["CREATE TABLE company (id INTEGER PRIMARY KEY, name {text} NOT NULL)"
   (str "CREATE TABLE person (id INTEGER PRIMARY KEY, name {text} NOT NULL, company_id INTEGER, "
        "active {bool}, note {text}, FOREIGN KEY (company_id) REFERENCES company (id))")
   "CREATE TABLE membership (group_code VARCHAR(20), member_code VARCHAR(20), role {text}, PRIMARY KEY (group_code, member_code))"
   "CREATE TABLE loose (id INTEGER, note {text})"
   "CREATE TABLE doc (id INTEGER PRIMARY KEY, name {text}, data {json})"
   "INSERT INTO company VALUES (1, 'Acme'), (2, 'Globex'), (3, 'Initech')"
   (str "INSERT INTO person VALUES (1, 'Ann', 1, TRUE, NULL), (2, 'Bob', 1, FALSE, NULL), "
        "(3, 'Cy', 2, TRUE, NULL), (4, 'Di', 3, FALSE, NULL)")
   "INSERT INTO membership VALUES ('g1', 'm1', 'a'), ('g1', 'm2', 'a'), ('g2', 'm1', 'a')"
   "INSERT INTO loose VALUES (1, 'x'), (1, 'y')"
   (str "INSERT INTO doc VALUES "
        "(1, 'Ann', '{\"plan\": \"free\", \"seats\": 3, \"vip\": false, \"address\": {\"city\": \"Oslo\"}, "
        "\"tags\": [\"a\", \"b\"], \"items\": [{\"sku\": \"x\", \"qty\": 1}, {\"sku\": \"y\", \"qty\": 2}]}'), "
        "(2, 'Bob', '{\"plan\": \"pro\", \"seats\": 5}'), "
        "(3, 'Cy', NULL)")])

(def ^:private tables
  {"company" [:id] "person" [:id] "membership" [:group_code :member_code] "loose" [:id :note] "doc" [:id]})

(defn- changed-by
  "Runs `expression` as a write, and returns what it changed in every table,
  and its result rows."
  [db expression]
  (let [before (e/snapshot db tables)
        result (e/write! db expression)]
    {:result result :changes (e/changes before (e/snapshot db tables))}))

(defn- unchanged-after-refusal
  "Runs `expression`, which must be refused, and asserts that nothing
  changed. Returns the error."
  ([db expression] (unchanged-after-refusal db expression {}))
  ([db expression opts]
   (let [before (e/snapshot db tables)
         error (e/refused db expression opts)]
     (is (some? error) (str expression " should have been refused"))
     (is (= {} (e/changes before (e/snapshot db tables))) "and nothing changed")
     error)))

(defn- scenario [f] (e/on-every-engine fixture f))

(deftest test-changes
  (testing "the comparison the scenarios rely on names every row that differs, and only those"
    (let [before {"t" {[1] {:id 1 :a "x"} [2] {:id 2 :a "y"} [3] {:id 3 :a "z"}}}
          after {"t" {[1] {:id 1 :a "x"} [2] {:id 2 :a "Y"} [4] {:id 4 :a "w"}}}]
      (is (= {"t" {:changed {[2] {:a ["y" "Y"]}} :deleted #{[3]} :added #{[4]}}}
             (e/changes before after)))
      (is (= {} (e/changes before before))))))

;; ---------------------------------------------------------------------------
;; update!
;; ---------------------------------------------------------------------------

(deftest test-update-one-row
  (scenario
   (fn [db]
     (testing "by its key: that row, that column, and nothing else"
       (is (= {:result [["person" 1]]
               :changes {"person" {:changed {[1] {:name ["Ann" "Anna"]}}}}}
              (changed-by db "person | where: id = 1 | update! name = 'Anna'")))))))

(deftest test-update-rows-by-condition
  (scenario
   (fn [db]
     (testing "every row the condition selects"
       (is (= {:result [["person" 2]]
               :changes {"person" {:changed {[1] {:note [nil "acme"]} [2] {:note [nil "acme"]}}}}}
              (changed-by db "person | where: company_id = 1 | update! note = 'acme'"))))

     (testing "several columns at once"
       (is (= {"person" {:changed {[3] {:name ["Cy" "Cyd"] :note [nil "n"]}}}}
              (:changes (changed-by db "person | where: id = 3 | update! name = 'Cyd', note = 'n'")))))

     (testing "a condition no row meets changes nothing"
       (is (= {:result [["person" 0]] :changes {}}
              (changed-by db "person | where: id = 99 | update! note = 'x'")))))))

(deftest test-update-values
  (scenario
   (fn [db]
     (testing "a quote, written twice in Pine, is stored once"
       (is (= {"person" {:changed {[1] {:note [nil "O'Brien"]}}}}
              (:changes (changed-by db "person | where: id = 1 | update! note = 'O''Brien'")))))

     (testing "text that looks like SQL is stored as text"
       (is (= {"person" {:changed {[2] {:note [nil "x'; DROP TABLE person; --"]}}}}
              (:changes (changed-by db "person | where: id = 2 | update! note = 'x''; DROP TABLE person; --'")))))

     (testing "letters outside ASCII, and an emoji"
       (is (= {"person" {:changed {[3] {:note [nil "Zoë 東京 😀"]}}}}
              (:changes (changed-by db "person | where: id = 3 | update! note = 'Zoë 東京 😀'")))))

     (testing "null clears a value"
       (is (= {"person" {:changed {[1] {:note ["O'Brien" nil]}}}}
              (:changes (changed-by db "person | where: id = 1 | update! note = null")))))

     (testing "a boolean column takes true and false as booleans"
       (let [{:keys [changes]} (changed-by db "person | where: id = 1 | update! active = false")
             [before after] (get-in changes ["person" :changed [1] :active])]
         ;; MySQL and SQLite keep a boolean as 1 and 0.
         (is (contains? #{true 1} before))
         (is (contains? #{false 0} after)))))))

(deftest test-update-composite-key
  (scenario
   (fn [db]
     (testing "a table keyed on two columns: exactly the row both name"
       (is (= {:result [["membership" 1]]
               :changes {"membership" {:changed {["g1" "m1"] {:role ["a" "b"]}}}}}
              (changed-by db "membership | where: group_code = 'g1' | where: member_code = 'm1' | update! role = 'b'")))))))

(deftest test-update-after-a-join
  (scenario
   (fn [db]
     (testing "the rows of the last table that the join reaches, and no other table"
       (is (= {"person" {:changed {[1] {:note [nil "acme"]} [2] {:note [nil "acme"]}}}}
              (:changes (changed-by db "company | where: id = 1 | person | update! note = 'acme'")))))

     (testing "two tables in one update!: each changes only its own rows"
       (is (= {"company" {:changed {[2] {:name ["Globex" "Globex2"]}}}
               "person" {:changed {[3] {:note [nil "g"]}}}}
              (:changes (changed-by db "company as c | where: c.id = 2 | person | update! c.name = 'Globex2', note = 'g'"))))))))

(deftest test-update-is-all-or-nothing
  (scenario
   (fn [db]
     (testing "when one table's update fails, the other table's is rolled back"
       ;; person.name is NOT NULL, so the second UPDATE fails after the
       ;; first has run.
       (unchanged-after-refusal db "company as c | where: c.id = 1 | person | update! c.name = 'Changed', name = null")))))

(deftest test-update-after-limit
  (scenario
   (fn [db]
     (testing "only the rows the limit keeps"
       ;; MySQL can't take LIMIT in an IN subquery, nor update a table it
       ;; reads in a subquery; Pine wraps the subquery for it.
       (is (= {"person" {:changed {[4] {:note [nil "last"]} [3] {:note [nil "last"]}}}}
              (:changes (changed-by db "person | order: id desc | limit: 2 | update! note = 'last'"))))))))

(deftest test-update-from-the-grid
  (scenario
   (fn [db]
     (testing "the update the results grid writes for a cell: from the cell's table, by its key"
       (is (= {"person" {:changed {[2] {:name ["Bob" "Robert"]}}}}
              (:changes (changed-by db "company | person | from: p_1 | where: id = 2 | update! p_1.name = 'Robert'")))))

     (testing "a cell of the first table of a join"
       (is (= {"company" {:changed {[1] {:name ["Acme" "Acme Inc"]}}}}
              (:changes (changed-by db "company | person | from: c_0 | where: id = 1 | update! c_0.name = 'Acme Inc'"))))))))

;; ---------------------------------------------------------------------------
;; update! into a key inside a JSON column
;; ---------------------------------------------------------------------------

(defn- data-after
  "The data column of doc `id` after `expression`, and asserts nothing else
  changed."
  [db id expression]
  (let [{:keys [changes]} (changed-by db expression)]
    (is (= #{[id]} (set (keys (get-in changes ["doc" :changed])))) "only that doc")
    (is (= #{"doc"} (set (keys changes))) "only the doc table")
    (is (= [:data] (keys (get-in changes ["doc" :changed [id]]))) "only its data column")
    (second (get-in changes ["doc" :changed [id] :data]))))

(deftest test-update-json-keys
  (scenario
   (fn [db]
     (testing "a key's value is replaced, and the rest of the JSON value is kept"
       (is (= {"plan" "team" "seats" 3 "vip" false "address" {"city" "Oslo"} "tags" ["a" "b"]
               "items" [{"sku" "x" "qty" 1} {"sku" "y" "qty" 2}]}
              (data-after db 1 "doc | where: id = 1 | update! data.plan = 'team'")))
       (is (= "string" (e/json-type db "doc" "data" "id" 1 ["plan"]))))

     (testing "a number stays a number, and a boolean a boolean"
       (data-after db 1 "doc | where: id = 1 | update! data.seats = 12")
       (is (= "number" (e/json-type db "doc" "data" "id" 1 ["seats"])))
       (data-after db 1 "doc | where: id = 1 | update! data.vip = true")
       (is (= "boolean" (e/json-type db "doc" "data" "id" 1 ["vip"]))))

     (testing "a nested key, and an element of an array by its index"
       (is (= {"city" "Bergen"} (get (data-after db 1 "doc | where: id = 1 | update! data.address.city = 'Bergen'") "address")))
       (is (= ["a" "z"] (get (data-after db 1 "doc | where: id = 1 | update! data.tags[1] = 'z'") "tags")))
       (is (= [{"sku" "x" "qty" 1} {"sku" "y" "qty" 7}]
              (get (data-after db 1 "doc | where: id = 1 | update! data.items[1].qty = 7") "items"))))

     (testing "a quote in the value is stored once"
       (is (= "it's" (get (data-after db 2 "doc | where: id = 2 | update! data.plan = 'it''s'") "plan"))))

     (testing "two keys in one update!"
       (is (= {"plan" "ent" "seats" 9}
              (select-keys (data-after db 2 "doc | where: id = 2 | update! data.plan = 'ent', data.seats = 9") ["plan" "seats"])))))))

(deftest test-update-json-where-there-is-no-key
  (scenario
   (fn [db]
     (testing "a JSON column that is NULL, or a key whose parent is missing, isn't written: 0 rows"
       (is (= {:result [["doc" 0]] :changes {}}
              (changed-by db "doc | where: id = 3 | update! data.plan = 'x'")))
       (is (= {:result [["doc" 0]] :changes {}}
              (changed-by db "doc | where: id = 2 | update! data.extra.deep = 'x'")))))))

;; ---------------------------------------------------------------------------
;; delete!
;; ---------------------------------------------------------------------------

(deftest test-delete
  (scenario
   (fn [db]
     (testing "the rows the condition selects"
       (is (= {:result [[2]] :changes {"person" {:deleted #{[2] [4]}}}}
              (changed-by db "person | where: active = false | delete! .id"))))

     (testing "after a join: rows of the last table only"
       (is (= {"person" {:deleted #{[3]}}}
              (:changes (changed-by db "company | where: id = 2 | person | delete! .id")))))

     (testing "after limit:, only the rows it keeps"
       (is (= {"membership" {:deleted #{["g2" "m1"]}}}
              (:changes (changed-by db "membership | order: group_code desc | limit: 1 | delete! .group_code")))))

     (testing "a condition no row meets deletes nothing"
       (is (= {:result [[0]] :changes {}}
              (changed-by db "person | where: id = 99 | delete! .id")))))))

;; ---------------------------------------------------------------------------
;; What must not change anything
;; ---------------------------------------------------------------------------

(deftest test-refusals
  (scenario
   (fn [db]
     (testing "a table without a primary key: its rows can't be told apart"
       (is (re-find #"no primary key" (unchanged-after-refusal db "loose | where: note = 'x' | update! note = 'z'"))))

     (testing "a caller that asked not to write, as an agent does"
       (unchanged-after-refusal db "person | where: id = 1 | update! name = 'X'" {:allow-writes false})
       (unchanged-after-refusal db "person | where: id = 1 | delete! .id" {:allow-writes false}))

     (testing "a write with no where: or limit:, which would change every row"
       (is (re-find #"every row" (unchanged-after-refusal db "person | update! note = 'x'")))
       (is (re-find #"every row" (unchanged-after-refusal db "person | delete! .id"))))

     (testing "a join that nothing resolves"
       (is (re-find #"No relation" (unchanged-after-refusal db "doc | where: id = 1 | membership | update! role = 'x'"))))

     (testing "a delete the database refuses, a company with people, leaves everything as it was"
       (unchanged-after-refusal db "company | where: id = 1 | delete! .id"))

     (testing "a value the column can't hold"
       (unchanged-after-refusal db "person | where: id = 1 | update! name = null")))))
