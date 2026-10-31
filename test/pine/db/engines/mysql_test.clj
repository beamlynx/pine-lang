(ns pine.db.engines.mysql-test
  "What only MySQL has, on a real MySQL server: its connection, and how Pine
  reads its schema from information_schema. What every engine does alike is
  in the topic files of test/pine/db (see pine.db.engines).

  Runs when PINE_TEST_MYSQL_URL names a MySQL server (see pine.db.engines,
  and scripts/test-databases.sh to start one)."
  (:require [clojure.test :refer [deftest is testing]]
            [pine.api :as api]
            [pine.db.connections :as connections]
            [pine.db.engines :as e]
            [pine.db.main :as db]))

(def ^:private fixture
  [(str "CREATE TABLE company (id INTEGER PRIMARY KEY, name VARCHAR(50) NOT NULL, created_at DATETIME, "
        "active BOOLEAN, data JSON, price DECIMAL(10, 2))")
   "CREATE TABLE person (id INTEGER PRIMARY KEY, company_id INTEGER, FOREIGN KEY (company_id) REFERENCES company (id))"
   "CREATE TABLE membership (group_code VARCHAR(20), member_code VARCHAR(20), PRIMARY KEY (group_code, member_code))"
   "CREATE TABLE loose (id INTEGER, note VARCHAR(20))"
   "INSERT INTO company VALUES (1, 'Acme', '2024-01-31 10:00:00', TRUE, '{\"a\": 1}', 9.50)"])

(deftest test-connection
  (e/on-engine
   :mysql fixture
   (fn [{:keys [id]}]
     (testing "the dialect is read from the connection"
       (is (= :mysql (connections/get-dialect id))))

     (testing "the connect check runs"
       (is (= id (:connection-id (api/test-connection id))))))))

(deftest test-schema-introspection
  (e/on-engine
   :mysql fixture
   (fn [{:keys [id]}]
     (let [references (db/init-references id)
           type-of (fn [table column]
                     (some #(when (= column (:column %)) (:type %)) (get-in references [:table table :columns])))]
       (testing "column types, as information_schema names them"
         (is (= ["int" "varchar" "datetime" "tinyint" "json" "decimal"]
                (map #(type-of "company" %) ["id" "name" "created_at" "active" "data" "price"]))))

       (testing "NOT NULL is read"
         (is (= "NO" (:nullable (first (filter #(= "name" (:column %)) (get-in references [:table "company" :columns])))))))

       (testing "primary keys, in key order, and none for a table without one"
         (is (= ["id"] (get-in references [:table "company" :primary-key])))
         (is (= ["group_code" "member_code"] (get-in references [:table "membership" :primary-key])))
         (is (nil? (get-in references [:table "loose" :primary-key]))))

       (testing "a foreign key, written as FOREIGN KEY (...) REFERENCES, becomes a join"
         (is (= [{:child "company_id" :parent "id"}]
                (:columns (first (get-in references [:table "person" :refers-to "company" :via "company_id"]))))))))))

(deftest test-values
  (e/on-engine
   :mysql fixture
   (fn [db]
     (testing "a BOOLEAN is a TINYINT, and comes back as true or 1"
       (is (contains? #{true 1} (first (e/column db "company | select: active")))))

     (testing "a DECIMAL keeps its digits"
       (is (= [9.50M] (e/column db "company | select: price")))))))
