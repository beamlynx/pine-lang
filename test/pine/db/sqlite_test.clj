(ns pine.db.sqlite-test
  "SQLite against a real file. Unlike Postgres and MySQL there is no server to
  start, so these run against an actual database on every `clojure -M:test`:
  introspection, generated SQL, and the connection's safety settings are
  checked end to end rather than by string comparison alone."
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [pine.api :as api]
   [pine.ast.main :as ast]
   [pine.db.connections :as connections]
   [pine.db.main :as db]
   [pine.db.sqlite :as sqlite]
   [pine.eval :as eval]
   [pine.parser :as parser])
  (:import (java.io File)
           (java.sql DriverManager)))

(def ^:private schema
  ["CREATE TABLE company (id INTEGER PRIMARY KEY, name TEXT NOT NULL, created_at DATETIME)"
   "CREATE TABLE person (id INTEGER PRIMARY KEY, name VARCHAR(255), company_id INTEGER REFERENCES company(id), active BOOLEAN)"
   "CREATE TABLE tag (id INTEGER PRIMARY KEY, label TEXT)"
   ;; REFERENCES tag, with no column: it means tag's primary key.
   "CREATE TABLE person_tag (person_id INTEGER, tag_id INTEGER, PRIMARY KEY (person_id, tag_id), FOREIGN KEY (person_id) REFERENCES person(id), FOREIGN KEY (tag_id) REFERENCES tag)"
   "CREATE TABLE a_comp (x INTEGER, y INTEGER, PRIMARY KEY (x, y))"
   "CREATE TABLE b_comp (id INTEGER PRIMARY KEY, ax INTEGER, ay INTEGER, FOREIGN KEY (ax, ay) REFERENCES a_comp(x, y))"
   ;; No declared types at all, which SQLite allows.
   "CREATE TABLE untyped (id, note)"
   "CREATE TABLE doc (id INTEGER PRIMARY KEY, name TEXT, data JSON)"
   "CREATE TABLE membership (group_code TEXT, member_code TEXT, role TEXT, PRIMARY KEY (group_code, member_code))"
   "INSERT INTO company VALUES (1, 'Acme', '2024-01-31 10:00:00'), (2, 'Globex', '2024-02-15 08:30:00')"
   "INSERT INTO person VALUES (1, 'Ann', 1, 1), (2, 'Bob', 1, 0), (3, 'Cy', 2, 1)"
   ;; Bob's age is a JSON string, not a number; Cy's country a JSON null.
   "INSERT INTO doc VALUES
      (1, 'Ann', '{\"country\":\"SE\",\"age\":31,\"vip\":true,\"address\":{\"city\":\"Oslo\"},\"tags\":[\"a\",\"b\"],\"home address\":\"x\"}'),
      (2, 'Bob', '{\"country\":\"NO\",\"age\":\"31\",\"vip\":false,\"address\":{\"city\":\"Bergen\"}}'),
      (3, 'Cy', '{\"country\":null,\"age\":20}'),
      (4, 'Di', NULL)"
   "INSERT INTO membership VALUES ('g1', 'm1', 'a'), ('g1', 'm2', 'a'), ('g2', 'm1', 'a')"])

(def ^:dynamic *db-file* nil)
(def ^:dynamic *id* nil)

(defn- with-database [f]
  (let [file (File/createTempFile "pine-sqlite-test" ".db")]
    (try
      (with-open [conn (DriverManager/getConnection (str "jdbc:sqlite:" (.getAbsolutePath file)))]
        (doseq [sql schema]
          (.execute (.createStatement conn) sql)))
      (let [id (with-redefs [connections/sqlite-enabled? (constantly true)]
                 (connections/add-connection-pool {:dbtype "sqlite" :dbname (.getAbsolutePath file)}))]
        (try
          (binding [*db-file* file *id* id]
            (f))
          (finally
            (connections/remove-connection-pool id)
            (db/clear-connection-if id))))
      (finally
        (.delete file)))))

(use-fixtures :once with-database)

(defn- generate [expression]
  (-> expression
      parser/parse
      :result
      (ast/generate *id* nil nil {} [])
      eval/build-query))

(defn- rows
  "The result rows of a pine expression, without the header row."
  [expression]
  (rest (db/run-query *id* (generate expression))))

(defn- selected
  "The first selected column of a pine expression's rows. A pine row is the
  selected columns followed by hidden id columns, so `first` is the value."
  [expression]
  (map first (rows expression)))

(defn- relation
  "The first relation from child -> parent over `via`."
  [child parent via]
  (first (get-in (db/init-references *id*) [:table child :refers-to parent :via via])))

(defn- column [table column-name]
  (let [references (db/init-references *id*)]
    (some #(when (= column-name (:column %)) %) (get-in references [:table table :columns]))))

(deftest test-connection
  (testing "the id is a route-safe name, and the dialect is read from the URL"
    (is (re-matches #"sqlite:pine-sqlite-test[0-9]+\.db:[0-9a-f]{10}" *id*))
    (is (= :sqlite (connections/get-dialect *id*))))

  (testing "the connect check runs on SQLite (it used to be SELECT NOW())"
    (is (= *id* (:connection-id (api/test-connection *id*)))))

  (testing "a connection count is reported"
    (is (= 1 (db/get-connection-count *id*)))))

(deftest test-validate-sqlite-connection
  (let [path (.getAbsolutePath ^File *db-file*)
        problem (fn [config]
                  (try (connections/validate-connection config) nil
                       (catch clojure.lang.ExceptionInfo e [(.getMessage e) (ex-data e)])))]
    (testing "refused unless the desktop app has turned SQLite on"
      (let [[message data] (problem {:dbtype "sqlite" :dbname path})]
        (is (= "bad-connection" (:error-type data)))
        (is (re-find #"desktop app" message))))

    (with-redefs [connections/sqlite-enabled? (constantly true)]
      (testing "an existing file passes, with no host, port or login"
        (is (= {:dbtype "sqlite" :dbname (.getCanonicalPath ^File *db-file*)
                :host nil :port nil :schema nil :user nil :password nil}
               (connections/validate-connection {:dbtype "sqlite" :dbname path}))))

      (testing "anything that is not a plain, existing, absolute file path is refused"
        (doseq [dbname [nil "" "  " "relative.db" "./app.db" ":memory:" "file:app.db"
                        (str path "?mode=ro") (str path ";x") (str path "\u0000")
                        (str path ".missing") (.getParent ^File *db-file*)]]
          (is (= "bad-connection" (:error-type (second (problem {:dbtype "sqlite" :dbname dbname}))))
              (pr-str dbname)))))))

(deftest test-schema-introspection
  (testing "declared types are reduced to the names pine matches on"
    (is (= "integer" (:type (column "person" "id"))))
    (is (= "text" (:type (column "person" "name"))) "VARCHAR(255) -> text")
    (is (= "boolean" (:type (column "person" "active"))))
    (is (= "datetime" (:type (column "company" "created_at")))))

  (testing "a column with no declared type has an unknown (nil) type, not a type called \"\""
    (is (contains? (column "untyped" "note") :type))
    (is (nil? (:type (column "untyped" "note")))))

  (testing "NOT NULL is read"
    (is (= "NO" (:nullable (column "company" "name"))))
    (is (= "YES" (:nullable (column "person" "name")))))

  (testing "a foreign key becomes a join"
    (is (= [{:child "company_id" :parent "id"}]
           (:columns (relation "person" "company" "company_id")))))

  (testing "REFERENCES with no column points at the parent's primary key"
    (is (= [{:child "tag_id" :parent "id"}]
           (:columns (relation "person_tag" "tag" "tag_id")))))

  (testing "a composite key is one relation over both columns, in order"
    (is (= [{:child "ax" :parent "x"} {:child "ay" :parent "y"}]
           (:columns (relation "b_comp" "a_comp" "ax")))))

  (testing "SQLite's own tables are not offered"
    (is (nil? (get-in (db/init-references *id*) [:table "sqlite_master"])))
    (is (nil? (get-in (db/init-references *id*) [:table "sqlite_sequence"])))))

(deftest test-affinity-type
  (let [affinity #'sqlite/affinity-type]
    (testing "names pine already knows pass through, lowercased and without a length"
      (is (= "boolean" (affinity "BOOLEAN")))
      (is (= "datetime" (affinity "DATETIME")))
      (is (= "date" (affinity "Date")))
      (is (= "json" (affinity "JSON"))))
    (testing "everything else follows SQLite's own affinity rules, in order"
      (is (= "integer" (affinity "UNSIGNED BIG INT")))
      (is (= "integer" (affinity "INT8")))
      (is (= "text" (affinity "VARCHAR(255)")))
      (is (= "text" (affinity "NVARCHAR(100)")))
      (is (= "text" (affinity "CLOB")))
      (is (= "blob" (affinity "BLOB")))
      (is (= "real" (affinity "DOUBLE PRECISION")))
      (is (= "real" (affinity "FLOAT")))
      (is (= "numeric" (affinity "DECIMAL(10, 5)")))
      (is (= "numeric" (affinity "SOMETHING ELSE"))))
    (testing "no declared type is unknown"
      (is (nil? (affinity nil)))
      (is (nil? (affinity "")))
      (is (nil? (affinity "  "))))))

(deftest test-queries
  (testing "a foreign-key join"
    (is (= [["Ann" "Acme"] ["Bob" "Acme"] ["Cy" "Globex"]]
           (map (fn [[a b]] [a b])
                (rows "person as p | company as c | select: p.name, c.name")))))

  (testing "ILIKE runs as LIKE, case-insensitively"
    (is (= ["Ann"] (selected "person | where: name ilike 'a%' | select: name"))))

  (testing "a date compares as the text it is stored as"
    (is (= ["Globex"]
           (selected "company | where: created_at > '2024-02-01' | select: name")))
    (is (= ["Acme"]
           (selected "company | where: created_at = '2024-01-31 10:00:00' | select: name")))
    (is (= ["Acme"]
           (selected "company | where: created_at < '2024-02-01T00:00' | select: name"))
        "a `T` between date and time matches SQLite's space"))

  (testing "date buckets: the week starts on Monday"
    ;; 2024-01-31 is a Wednesday, 2024-02-15 a Thursday.
    (is (= ["2024" "2024"] (selected "company | select: created_at => year")))
    (is (= ["2024-01" "2024-02"] (selected "company | select: created_at => month")))
    (is (= ["2024-01-29" "2024-02-12"] (selected "company | select: created_at => week")))
    (is (= ["2024-01-31 10" "2024-02-15 08"] (selected "company | select: created_at => hour")))
    (is (= ["2024-01-31 10:00" "2024-02-15 08:30"] (selected "company | select: created_at => minute"))))

  (testing "a boolean column"
    (is (= ["Ann" "Cy"] (selected "person | where: active = true | select: name"))))

  (testing "group and count"
    (is (= [[1 2] [2 1]] (rows "person | select: company_id | group: company_id => count")))
    (is (= [[3]] (rows "person | count:")))))

(deftest test-writes
  (testing "foreign keys are enforced - this connection turns them on"
    (is (thrown-with-msg? Exception #"FOREIGN KEY constraint failed"
                          (db/run-sql *id* "DELETE FROM company WHERE id = 1"))))

  (testing "update! and delete! change the rows they select, and a transaction rolls back as a whole"
    (db/run-sql *id* "INSERT INTO tag VALUES (10, 'a'), (11, 'b'), (12, 'c')")
    (let [update (generate "tag | where: id = 10 | update! label = 'z'")]
      (is (= [["tag" 1]] (db/run-action-queries-in-transaction *id* (:queries update)))))
    (is (= ["z"] (selected "tag | where: id = 10 | select: label")))
    (let [delete (generate "tag | where: id = 11 | delete! .id")]
      (is (= 1 (db/run-action-query *id* delete))))
    (is (= [10 12] (selected "tag | select: id")))
    (is (thrown? Exception
                 (db/run-action-queries-in-transaction
                  *id* [{:table "tag" :query "DELETE FROM tag WHERE id = 10" :params []}
                        {:table "tag" :query "DELETE FROM nope" :params []}])))
    (is (= [10 12] (selected "tag | select: id")) "the first delete was rolled back")
    (db/run-sql *id* "DELETE FROM tag")))

(deftest test-connection-is-contained
  (testing "ATTACH is refused, so a query can't open another file - or step around an access policy"
    (is (thrown-with-msg? Exception #"too many attached databases"
                          (db/run-sql *id* (str "ATTACH DATABASE '" (.getAbsolutePath ^File *db-file*) "' AS other")))))

  (testing "extensions can't be loaded"
    (is (thrown? Exception (db/run-sql *id* "SELECT load_extension('anything')")))))

(deftest test-primary-keys
  (testing "a table's primary key is read, in key order, so update! and delete! can tell its rows apart"
    (let [references (db/init-references *id*)]
      (is (= ["group_code" "member_code"] (get-in references [:schema "main" :table "membership" :primary-key])))
      (is (= ["id"] (get-in references [:schema "main" :table "person" :primary-key])))
      (is (nil? (get-in references [:schema "main" :table "untyped" :primary-key])))))

  (testing "a composite key changes exactly the row it names"
    (let [update (generate "membership | where: group_code = 'g1' | where: member_code = 'm1' | update! role = 'b'")]
      (is (= [["membership" 1]] (db/run-action-queries-in-transaction *id* (:queries update)))))
    (is (= [["a"] ["a"] ["b"]] (sort-by (comp str first) (map vector (selected "membership | select: role | order: group_code, member_code desc")))))
    (db/run-sql *id* "UPDATE membership SET role = 'a'"))

  (testing "a table with no primary key can't be updated: its rows can't be told apart"
    (is (thrown-with-msg? Exception #"no primary key"
                          (generate "untyped | where: id = 1 | update! note = 'x'")))))

(deftest test-json-paths
  (testing "= compares typed JSON values: a string \"31\" is not the number 31"
    (is (= ["Ann"] (selected "doc | where: data.country = 'SE' | select: name")))
    (is (= ["Ann"] (selected "doc | where: data.age = 31 | select: name")))
    (is (= ["Ann"] (selected "doc | where: data.vip = true | select: name"))))

  (testing "< and > skip a value that isn't a number"
    (is (= ["Ann"] (selected "doc | where: data.age > 30 | select: name")))
    (is (= ["Cy"] (selected "doc | where: data.age < 30 | select: name"))))

  (testing "like and in compare text; is null is true for a JSON null and for a missing key"
    (is (= ["Ann"] (selected "doc | where: data.country like 'S%' | select: name")))
    (is (= ["Ann" "Bob"] (selected "doc | where: data.country in ('SE', 'NO') | select: name")))
    (is (= ["Cy" "Di"] (selected "doc | where: data.country is null | select: name"))))

  (testing "nested keys, indexes and quoted keys"
    (is (= [["Ann" "Oslo" "a"] ["Bob" "Bergen" nil] ["Cy" nil nil] ["Di" nil nil]]
           (map #(vec (take 3 %)) (rows "doc | select: name, data.address.city, data.tags[0]"))))
    (is (= ["x" nil nil nil] (selected "doc | select: data.'home address'"))))

  (testing "group by a key"
    (is (= #{[nil 2] ["NO" 1] ["SE" 1]}
           (set (rows "doc | select: data.country | group: data.country => count"))))))
