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
     "CREATE TABLE note (id INTEGER PRIMARY KEY, body TEXT, size INTEGER)"
     "INSERT INTO note VALUES (1, '[\"x\", \"y\"]', 2)"
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
   (fn [dialect id]
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

     (testing "On SQLite, which keeps JSON in text, a text column can be unnested, and is suggested"
       (if (= dialect :sqlite)
         (do (is (= #{["x"] ["y"]} (visible id "note | unnest: body as b | s: b")))
             (is (= ["body"] (map :column (get-in (api/api-build "note | unnest: " nil id) [:ast :hints :unnest])))))
         (is (re-find #"not a JSON column" (str (:error (api/api-eval "note | unnest: body as b" id)))))))

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

(defn- hidden-value
  "The hidden column `column-alias` of each row, by the visible value in
  column 0."
  [id expression column-alias]
  (let [{:keys [header rows]} (result id expression)
        i (.indexOf header column-alias)]
    (is (<= 0 i) (str column-alias " in " header))
    (set (map (fn [row] [(first row) (nth row i)]) rows))))

(deftest test-element-positions
  (each-database
   (fn [_ id]
     (testing "Each element's position in its array, from 0"
       (is (= #{["a" "0"] ["b" "1"]}
              (hidden-value id "doc | where: id = 1 | unnest: data.tags as t | s: t" "__t__index")))
       (is (= #{["p1" "0"] ["p2" "1"] ["p3" "0"]}
              (hidden-value id "doc | unnest: data.items as i | unnest: i.parts as p | s: p" "__p__index"))))

     (testing "and the JSON type of each value the grid can edit"
       (is (= #{["1" "number"] ["2" "number"] ["5" "number"]}
              (hidden-value id "doc | unnest: data.items as i | s: i.qty" "__i__qty__type"))))

     (testing "`e` by itself is labelled as the element, not as a column of the table"
       (is (= {:alias "t" :column "value" :path [] :column-alias "t"}
              (-> (result id "doc | where: id = 2 | unnest: data.tags as t") :columns last
                  (select-keys [:alias :column :path :column-alias])))))

     (testing "The update the grid writes for `i.qty` changes that one value"
       (result id "doc | unnest: data.items as i | from: d_0 | where: id = 1 | update! d_0.data.items[1].qty = 7")
       (is (= #{["x" "1"] ["y" "7"] ["x" "5"]}
              (visible id "doc | unnest: data.items as i | s: i.sku, i.qty")))))))
