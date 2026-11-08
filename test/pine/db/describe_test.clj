(ns pine.db.describe-test
  "`describe:` and `references:` on real SQLite, MySQL and Postgres
  databases: each engine's schema reading gives the same columns, keys and
  relations. See pine.db.engines."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [pine.db.engines :as e]))

(def ^:private fixture
  ["CREATE TABLE company (id INTEGER PRIMARY KEY, name {text} NOT NULL)"
   (str "CREATE TABLE person (id INTEGER PRIMARY KEY, name {text}, company_id INTEGER, "
        "FOREIGN KEY (company_id) REFERENCES company (id))")
   ;; No foreign key: Pine infers this one from the column's name.
   "CREATE TABLE note (id INTEGER PRIMARY KEY, person_id INTEGER)"
   "INSERT INTO company VALUES (1, 'Acme')"])

(defn- unqualified
  "Text without the schema in front of a table name: each engine names its
  own (`main`, `public`, the MySQL database)."
  [s]
  (str/replace s #"[\w$]+\.(company|person|note)\b" "$1"))

(defn- result
  "The rows of an expression, with schemas taken out."
  [db expression header]
  (let [{h :header rows :rows error :error} (e/pine db expression)]
    (is (nil? error))
    (is (= header h))
    (mapv #(mapv unqualified %) rows)))

(defn- described
  "The rows of `describe:` without the type, which each engine spells its
  own way."
  [db expression]
  (mapv (fn [[column _type & more]] (into [column] more))
        (result db expression ["column" "type" "key" "nullable" "masked"])))

(deftest test-describe
  (e/on-every-engine
   fixture
   (fn [db]
     (testing "a table's columns, with its key and which may be empty"
       (is (= [["id" "primary" "" ""]
               ["name" "" "" ""]]
              (described db "company | describe:"))))

     (testing "a nullable column says so"
       (is (= ["name" "" "yes" ""]
              (second (described db "person | describe:")))))

     (testing "the database isn't queried: the rows come from the schema"
       (is (= 1 (count (e/rows db "company"))))))))

(deftest test-references
  (e/on-every-engine
   fixture
   (fn [db]
     (testing "the relations into a table"
       (is (= [["person.company_id" "company.id" "foreign key" "| person .company_id"]]
              (result db "company | references:" ["from" "to" "how" "pine"]))))

     (testing "a foreign key out, and a relation in that Pine infers from a column's name"
       (is (= #{["person.company_id" "company.id" "foreign key" "| company .company_id :parent"]
                ["note.person_id" "person.id" "naming" "| note .person_id"]}
              (set (result db "person | references:" ["from" "to" "how" "pine"]))))))))
