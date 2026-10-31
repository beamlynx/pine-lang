(ns pine.db.engines.sqlite-test
  "What only SQLite has, on a real file: its connection and its safety
  settings, how it reports column types, dates kept as text, and the
  access policy's handling of its types and of case. What every engine
  does alike is in the topic files of test/pine/db, which run on SQLite too
  (see pine.db.engines)."
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
   ;; Declared types SQLite would give NUMERIC affinity.
   "CREATE TABLE contact (id INTEGER PRIMARY KEY, email STRING, ssn SSN, profile JSONB, score NUMERIC)"
   "INSERT INTO company VALUES (1, 'Acme', '2024-01-31 10:00:00'), (2, 'Globex', '2024-02-15 08:30:00')"
   "INSERT INTO person VALUES (1, 'Ann', 1, 1), (2, 'Bob', 1, 0), (3, 'Cy', 2, 1)"
   ;; Bob's age is a JSON string, not a number; Cy's country a JSON null.
   "INSERT INTO doc VALUES
      (1, 'Ann', '{\"country\":\"SE\",\"age\":31,\"vip\":true,\"address\":{\"city\":\"Oslo\"},\"tags\":[\"a\",\"b\"],\"home address\":\"x\"}'),
      (2, 'Bob', '{\"country\":\"NO\",\"age\":\"31\",\"vip\":false,\"address\":{\"city\":\"Bergen\"}}'),
      (3, 'Cy', '{\"country\":null,\"age\":20}'),
      (4, 'Di', NULL)"
   "INSERT INTO membership VALUES ('g1', 'm1', 'a'), ('g1', 'm2', 'a'), ('g2', 'm1', 'a')"
   "INSERT INTO contact VALUES (1, 'ann@example.com', '123-45-6789', '{}', 7)"])

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
    (testing "integer, real, decimal, text and blob names get those names"
      (is (= "integer" (affinity "UNSIGNED BIG INT")))
      (is (= "integer" (affinity "INT8")))
      (is (= "integer" (affinity "INT(11)")))
      (is (= "integer" (affinity "TINYINT(1)")))
      (is (= "integer" (affinity "BIGINT UNSIGNED")))
      (is (= "integer" (affinity "INT UNSIGNED")))
      (is (= "integer" (affinity "BIGINT(20) UNSIGNED")))
      (is (= "text" (affinity "VARCHAR(255)")))
      (is (= "text" (affinity "NVARCHAR(100)")))
      (is (= "text" (affinity "CLOB")))
      (is (= "blob" (affinity "BLOB")))
      (is (= "real" (affinity "DOUBLE PRECISION")))
      (is (= "real" (affinity "FLOAT")))
      (is (= "numeric" (affinity "DECIMAL(10, 5)"))))
    (testing "any other declared type stays as written, so no rule about numbers shows it"
      ;; SQLite gives these NUMERIC affinity, STRING included, and POINT
      ;; INTEGER affinity. An access policy that shows numeric columns
      ;; showed every one of them.
      (is (= "string" (affinity "STRING")))
      (is (= "jsonb" (affinity "JSONB")))
      (is (= "ssn" (affinity "SSN")))
      (is (= "point" (affinity "POINT")))
      (is (= "something else" (affinity "SOMETHING ELSE"))))
    (testing "no declared type is unknown"
      (is (nil? (affinity nil)))
      (is (nil? (affinity "")))
      (is (nil? (affinity "  "))))))

(deftest test-dates
  ;; SQLite has no date type: a date is the text it is stored as. The
  ;; queries every engine runs alike are in pine.db.queries-test.
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

  (testing "now() compares with the text"
    (is (= [[2]] (rows "company | where: created_at < now() | count:")))))

(deftest test-writes
  ;; What update! and delete! change on every engine is in
  ;; pine.db.writes-test. These are SQLite's own.
  (testing "foreign keys are enforced - this connection turns them on"
    (is (thrown-with-msg? Exception #"FOREIGN KEY constraint failed"
                          (db/run-sql *id* "DELETE FROM company WHERE id = 1"))))

  (testing "a transaction rolls back as a whole"
    (db/run-sql *id* "INSERT INTO tag VALUES (10, 'a'), (11, 'b'), (12, 'c')")
    (is (thrown? Exception
                 (db/run-action-queries-in-transaction
                  *id* [{:table "tag" :query "DELETE FROM tag WHERE id = 10" :params []}
                        {:table "tag" :query "DELETE FROM nope" :params []}])))
    (is (= [10 11 12] (selected "tag | select: id")) "the first delete was rolled back")
    (db/run-sql *id* "DELETE FROM tag"))

  (testing "a boolean is kept as 1 or 0"
    (db/run-action-queries-in-transaction *id* (:queries (generate "person | where: id = 1 | update! active = false")))
    (is (= [0] (selected "person | where: id = 1 | select: active")))
    (db/run-action-queries-in-transaction *id* (:queries (generate "person | where: id = 1 | update! active = true")))
    (is (= [1] (selected "person | where: id = 1 | select: active")))))

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
      (is (nil? (get-in references [:schema "main" :table "untyped" :primary-key]))))))

(defn- policy-rows
  "Like rows, under an access policy."
  [expression rules]
  (rest (db/run-query *id* (-> expression parser/parse :result
                               (ast/generate *id* nil nil {} rules)
                               eval/build-query))))

(deftest test-access-policy
  (let [show-numbers {:type "column-type" :allow ["integer" "numeric" "real"]}]
    (testing "a declared type SQLite would call numeric isn't shown by a rule about numbers"
      (is (= "string" (:type (column "contact" "email"))))
      (is (= [[1 "xxxxx" "xxxxx" "xxxxx" 7]]
             (map #(vec (take 5 %)) (policy-rows "contact | s: id, email, ssn, profile, score" [show-numbers])))))

    (testing "a hide rule covers a column however its name is spelled: SQLite ignores case"
      (let [rules [show-numbers
                   {:type "column-name" :suffix "name"}
                   {:type "hide" :match {:type "column" :table "person" :column "name"}}]]
        (doseq [e ["person | s: NAME" "PERSON | s: name" "Person | s: Name"]]
          (is (= ["xxxxx" "xxxxx" "xxxxx"] (map first (policy-rows e rules))) e))
        (is (thrown-with-msg? Exception #"`p_0.NAME` is a masked column"
                              (policy-rows "person | group: NAME with count" rules)))))

    (testing "a hand-written `column` rule matches whatever case it is written in"
      (is (= ["xxxxx" "xxxxx" "xxxxx"]
             (map first (policy-rows "person | s: name" [show-numbers
                                                         {:type "column-name" :suffix "name"}
                                                         {:type "hide" :match {:type "column" :table "PERSON" :column "Name"}}])))))

    (testing "a column the index doesn't have is refused, not guessed at"
      (is (thrown-with-msg? Exception #"`p_0.nickname` isn't a column of `person`"
                            (policy-rows "person | s: nickname" [show-numbers]))))))
