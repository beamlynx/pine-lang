(ns pine.db.unnest-test
  "`unnest:` on real databases: the same expressions, and the same rows, on
  SQLite, MySQL and Postgres. The SQL each one writes is tested as text in
  pine.json-paths-test; this checks that each database runs it and returns
  what Pine means.

  SQLite runs on every `clojure -M:test`, against a temporary file. MySQL
  and Postgres run when a server is named. The user must be allowed to
  create a database: each run creates one with a fresh name and drops it.

    docker run -d --rm --name pine_test_mysql -e MYSQL_ROOT_PASSWORD=pine -p 55436:3306 mysql:8
    docker run -d --rm --name pine_test_pg -e POSTGRES_USER=pine -e POSTGRES_PASSWORD=pine \\
      -e POSTGRES_DB=pine -p 55432:5432 postgres:15
    PINE_TEST_MYSQL_URL=mysql://root:pine@localhost:55436/mysql \\
    PINE_TEST_POSTGRES_URL=postgres://pine:pine@localhost:55432/pine \\
      clojure -M:test -n pine.db.unnest-test"
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as s]
            [clojure.test :refer [deftest is testing]]
            [pine.api :as api]
            [pine.db.connections :as connections]
            [pine.db.main :as db])
  (:import (java.io File)
           (java.net URI)))

(defn- schema
  "The fixture, in SQL all three databases read. A JSON column is `JSON`,
  which Postgres and MySQL have, and SQLite accepts as a declared type."
  [dialect]
  (let [json (if (= dialect :postgres) "jsonb" "JSON")]
    [(str "CREATE TABLE company (id INTEGER PRIMARY KEY, name VARCHAR(50))")
     (str "CREATE TABLE doc (id INTEGER PRIMARY KEY, name VARCHAR(50), data " json ")")
     "INSERT INTO company VALUES (1, 'Acme'), (2, 'Globex'), (3, 'Initech')"
     (str "INSERT INTO doc VALUES "
          "(1, 'Ann', '{\"tags\": [\"a\", \"b\"], \"items\": [{\"sku\": \"x\", \"qty\": 1, \"parts\": [\"p1\", \"p2\"]}, {\"sku\": \"y\", \"qty\": 2, \"parts\": []}], \"companyIds\": [1, 3], \"companies\": [{\"id\": 2}]}'), "
          "(2, 'Bob', '{\"tags\": [\"b\"], \"items\": [{\"sku\": \"x\", \"qty\": 5, \"parts\": [\"p3\"]}], \"companyIds\": [2], \"companies\": []}'), "
          "(3, 'Cy', '{\"tags\": \"not an array\"}'), "
          "(4, 'Di', NULL)")]))

(defn- server-spec
  "A clojure.java.jdbc spec for database `dbname` on the server at `url`."
  [dialect ^URI uri dbname]
  (let [[user password] (s/split (.getUserInfo uri) #":" 2)
        scheme (if (= dialect :mysql) "mysql" "postgresql")]
    {:connection-uri (str "jdbc:" scheme "://" (.getHost uri) ":" (.getPort uri) "/" dbname
                          "?user=" user "&password=" password)
     :user user :password password :host (.getHost uri) :port (.getPort uri)}))

(defn- with-server-database
  "Run (f connection-id) on a fresh database on the server, filled with the
  fixture, and drop it after."
  [dialect url f]
  (let [uri (URI. url)
        admin (server-spec dialect uri (subs (.getPath uri) 1))
        dbname (str "pine_unnest_" (System/currentTimeMillis))
        drop-sql (str "DROP DATABASE IF EXISTS " dbname (when (= dialect :postgres) " WITH (FORCE)"))]
    (jdbc/execute! admin [(str "CREATE DATABASE " dbname)] {:transaction? false})
    (try
      (let [{:keys [host port user password] :as spec} (server-spec dialect uri dbname)]
        (doseq [statement (schema dialect)]
          (jdbc/execute! spec [statement]))
        (let [id (connections/add-connection-pool {:dbtype (name dialect) :host host :port port :dbname dbname
                                                   :user user :password password})]
          (try (f id)
               (finally
                 (connections/remove-connection-pool id)
                 (db/clear-connection-if id)))))
      (finally
        (jdbc/execute! admin [drop-sql] {:transaction? false})))))

(defn- with-sqlite-database [f]
  (let [file (File/createTempFile "pine-unnest-test" ".db")]
    (try
      (let [spec {:connection-uri (str "jdbc:sqlite:" (.getAbsolutePath file))}]
        (doseq [statement (schema :sqlite)]
          (jdbc/execute! spec [statement])))
      (let [id (with-redefs [connections/sqlite-enabled? (constantly true)]
                 (connections/add-connection-pool {:dbtype "sqlite" :dbname (.getAbsolutePath file)}))]
        (try (f id)
             (finally
               (connections/remove-connection-pool id)
               (db/clear-connection-if id))))
      (finally
        (.delete file)))))

(def ^:private databases
  "[dialect run], where run calls (f connection-id) on a fresh fixture. A
  server that isn't named is left out."
  (let [mysql (System/getenv "PINE_TEST_MYSQL_URL")
        postgres (System/getenv "PINE_TEST_POSTGRES_URL")]
    (cond-> [[:sqlite with-sqlite-database]]
      mysql (conj [:mysql (partial with-server-database :mysql mysql)])
      postgres (conj [:postgres (partial with-server-database :postgres postgres)]))))

(defn- result
  "{:header :rows :columns} of an expression. Values are strings, or nil:
  each database gives a JSON number back as its own type."
  [id expression]
  (let [{:keys [error result columns]} (api/api-eval expression id)]
    (is (nil? error) (str expression ": " error))
    {:header (mapv name (first result))
     :rows (mapv (fn [row] (mapv #(some-> % str) row)) (rest result))
     :columns columns}))

(defn- visible
  "The rows of an expression, without the hidden columns Pine adds."
  [id expression]
  (let [{:keys [rows columns]} (result id expression)
        shown (keep-indexed (fn [i c] (when-not (:hidden c) i)) columns)]
    (set (map (fn [row] (mapv #(nth row %) shown)) rows))))

(defn- each-database [f]
  (doseq [[dialect run] databases]
    (testing (name dialect)
      (run (fn [id] (f dialect id))))))

(deftest test-unnest
  (each-database
   (fn [_ id]
     (testing "One row for each element, next to the row it came from"
       (is (= #{["Ann" "a"] ["Ann" "b"] ["Bob" "b"]}
              (visible id "doc | unnest: data.tags as t | s: name, t"))))

     (testing "A row whose value isn't an array, or is null, has no elements"
       (is (= #{["Ann"] ["Bob"]}
              (visible id "doc | unnest: data.tags as t | s: name"))))

     (testing "Keys of an element, and a condition on one"
       (is (= #{["Ann" "1"] ["Bob" "5"]}
              (visible id "doc | unnest: data.items as i | where: i.sku = 'x' | s: name, i.qty"))))

     (testing "Elements of elements"
       (is (= #{["Ann" "p1"] ["Ann" "p2"] ["Bob" "p3"]}
              (visible id "doc | unnest: data.items as i | unnest: i.parts as p | s: name, p"))))

     (testing "Grouped by an element"
       (is (= #{["a" "1"] ["b" "2"]}
              (visible id "doc | unnest: data.tags as t | group: t with count"))))

     (testing "The pipe stays on the table: a bare name is its column"
       (is (= #{["2"]}
              (visible id "doc | unnest: data.tags as t | where: t = 'b' | where: name = 'Bob' | s: id")))))))

(deftest test-unnest-writes
  (each-database
   (fn [_ id]
     (testing "update! after unnest: changes the table's rows that have a matching element, once each"
       (result id "doc | unnest: data.tags as t | where: t = 'b' | update! name = 'Tagged'")
       (is (= #{["1" "Tagged"] ["2" "Tagged"] ["3" "Cy"] ["4" "Di"]}
              (visible id "doc | s: id, name"))))

     (testing "delete! after unnest: deletes them"
       (result id "doc | unnest: data.items as i | where: i.qty = 5 | delete! .id")
       (is (= #{["1"] ["3"] ["4"]}
              (visible id "doc | s: id")))))))

(deftest test-join-from-a-name
  (each-database
   (fn [_ id]
     (testing "Elements named `company_id` join `company` on each element"
       (is (= #{["Ann" "Acme"] ["Ann" "Initech"] ["Bob" "Globex"]}
              (visible id "doc | unnest: data.companyIds as company_id | company | s: d_0.name, name"))))

     (testing "Elements named after the table join it on their `id`"
       (is (= #{["Ann" "Globex"]}
              (visible id "doc | unnest: data.companies as company | company | s: d_0.name, name"))))

     (testing "A key inside JSON named with `as`"
       (is (= #{["1" "Acme"]}
              (visible id "doc | where: id = 1 | s: data.companyIds[0] as company_id | company | s: name"))))

     (testing "A plain column named with `as`"
       (is (= #{["1" "Ann" "Acme"] ["2" "Bob" "Globex"]}
              (visible id "doc | where: id < 3 | s: id as company_id | company | s: d_0.name, name")))))))
