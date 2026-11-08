(ns pine.db.describe-test
  "`describe:` on real SQLite, MySQL and Postgres databases: each engine's
  schema reading gives the same columns, keys and joins. See
  pine.db.engines."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [pine.db.engines :as e]))

(def ^:private fixture
  ["CREATE TABLE company (id INTEGER PRIMARY KEY, name {text} NOT NULL)"
   (str "CREATE TABLE person (id INTEGER PRIMARY KEY, name {text}, company_id INTEGER, "
        "FOREIGN KEY (company_id) REFERENCES company (id))")
   "INSERT INTO company VALUES (1, 'Acme')"])

(defn- unqualified
  "Text without the schema in front of a table name: each engine names its
  own (`main`, `public`, the MySQL database)."
  [s]
  (str/replace s #"[\w$]+\.(company|person)\b" "$1"))

(defn- described
  "The rows of `describe:` without the type, which each engine spells its
  own way, and without the line saying when the schema was read."
  [db expression]
  (let [{:keys [header rows error]} (e/pine db expression)]
    (is (nil? error))
    (is (= ["kind" "name" "type" "detail" "pine"] header))
    (->> rows
         (remove #(= "schema" (first %)))
         (mapv (fn [[kind name _type detail pine]]
                 (mapv unqualified [kind name detail pine]))))))

(deftest test-describe
  (e/on-every-engine
   fixture
   (fn [db]
     (testing "a table's columns, with its key and which may be empty, and the tables that join to it"
       (is (= [["column" "id" "primary key" "c_0.id"]
               ["column" "name" "" "c_0.name"]
               ["joined from" "person" "foreign key: person.company_id -> company.id" "| person .company_id"]]
              (described db "company | describe:"))))

     (testing "the table it joins to"
       (is (some #{["joins to" "company" "foreign key: person.company_id -> company.id" "| company .company_id :parent"]}
                 (described db "person | describe:"))))

     (testing "a nullable column says so"
       (is (= ["column" "name" "nullable" "p_0.name"]
              (second (described db "person | describe:")))))

     (testing "the database isn't queried: the rows come from the schema"
       (is (= 1 (count (e/rows db "company")))))

     (testing "when the schema was read"
       (is (re-find #"^Read at " (nth (last (:rows (e/pine db "company | describe:"))) 3)))))))
