(ns pine.db.engines.postgres-test
  "What only Postgres has: SQL to Pine, checked on a real Postgres. The SQL
  written for Postgres (UPDATE ... FROM, jsonb functions, ...) and the Pine
  it converts to pick the same rows.

  Runs when PINE_TEST_POSTGRES_URL names a Postgres server (see
  pine.db.engines, and scripts/test-databases.sh to start one). The user
  must be allowed to create a database: the test creates one with a fresh
  name, fills it, and drops it at the end."
  (:require [cheshire.core :as json]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as s]
            [clojure.test :refer [deftest is testing]]
            [pine.api :as api]
            [pine.db.connections :as connections]
            [pine.db.engines :as e]
            [pine.db.main :as db]
            [pine.sql.transpile :as t])
  (:import (java.net URI)))

(def ^:private server
  "Fails the run when CI requires the server and it isn't named. See
  pine.db.engines."
  (e/server-url :postgres))

;; The tenant the script works on, another tenant, the user whose edits
;; don't count, and a user whose edits do.
(def ^:private tenant "86b8ad91-7463-478a-aa43-43a977105bf4")
(def ^:private other-tenant "11111111-1111-1111-1111-111111111111")
(def ^:private excluded-user "d3b93066-ea59-4259-a534-943249fba79c")
(def ^:private user "22222222-2222-2222-2222-222222222222")

(defn- doc-id [code] (str "d0000000-0000-0000-0000-0000000000" code))
(defn- att-id [code] (str "a0000000-0000-0000-0000-0000000000" code))
(def ^:private company-1 "c0000000-0000-0000-0000-000000000001")
(def ^:private company-2 "c0000000-0000-0000-0000-000000000002")

;; Each document: code, company, tenant, reference_id, created_at (as a day
;; of October 2026), archived?, deleted?. A pair or more with the same
;; dedupe key is one group, and all but one of each group are deleted.
(def ^:private documents
  [;; Two by reference_id, neither edited: the newest is kept.
   ["a1" company-1 tenant "R1" 1 false false]
   ["a2" company-1 tenant "R1" 2 false false]
   ;; The older one is edited: it is kept.
   ["b1" company-1 tenant "R2" 1 false false]
   ["b2" company-1 tenant "R2" 2 false false]
   ;; By attachment only: both have the SharePoint attachment 01.
   ["d1" company-1 tenant nil 1 false false]
   ["d2" company-1 tenant nil 2 false false]
   ;; Only an upload attachment and no reference: not a SharePoint document.
   ["d3" company-1 tenant nil 3 false false]
   ;; SharePoint attachment 02 and upload 03: 02 is preferred, so it groups
   ;; with d5, which has only 02.
   ["d4" company-1 tenant nil 4 false false]
   ["d5" company-1 tenant nil 5 false false]
   ;; Edited only by the excluded user: counts as not edited.
   ["e1" company-1 tenant "R3" 1 false false]
   ["e2" company-1 tenant "R3" 2 false false]
   ;; Only downloaded: counts as not edited.
   ["f1" company-1 tenant "R4" 1 false false]
   ["f2" company-1 tenant "R4" 2 false false]
   ;; Both edited: the newest edited is kept.
   ["01" company-1 tenant "R5" 1 false false]
   ["02" company-1 tenant "R5" 2 false false]
   ;; One archived: it isn't ranked, nor deleted.
   ["03" company-1 tenant "R6" 1 true false]
   ["04" company-1 tenant "R6" 2 false false]
   ;; Another tenant's duplicates: untouched.
   ["05" company-1 other-tenant "R1" 1 false false]
   ["06" company-1 other-tenant "R1" 2 false false]
   ;; Already deleted: left out.
   ["07" company-1 tenant "R7" 1 false true]
   ["08" company-1 tenant "R7" 2 false false]
   ;; The same reference in two companies: two groups.
   ["09" company-1 tenant "R8" 1 false false]
   ["0a" company-2 tenant "R8" 2 false false]
   ;; Edited, but the activity is another tenant's: counts as not edited.
   ["0b" company-1 tenant "R9" 1 false false]
   ["0c" company-1 tenant "R9" 2 false false]
   ;; The same time: the smaller id is kept.
   ["0d" company-1 tenant "R10" 1 false false]
   ["0e" company-1 tenant "R10" 1 false false]
   ;; Three, the middle one edited.
   ["11" company-1 tenant "R11" 1 false false]
   ["12" company-1 tenant "R11" 2 false false]
   ["13" company-1 tenant "R11" 3 false false]
   ;; No attachment and no reference: not a SharePoint document.
   ["14" company-1 tenant nil 1 false false]
   ;; Edited by an activity with no user and no type: that counts.
   ["15" company-1 tenant "R12" 1 false false]
   ["16" company-1 tenant "R12" 2 false false]])

(def ^:private attachments
  [["01" "sharepoint"] ["02" "sharepoint"] ["03" "upload"] ["04" "upload"]])

(def ^:private links
  [["d1" "01"] ["d2" "01"] ["d3" "04"] ["d4" "02"] ["d4" "03"] ["d5" "02"]])

;; Activity on a document: code, tenant, user, type.
(def ^:private activities
  [["b1" tenant user "company-document-updated"]
   ["e1" tenant excluded-user "company-document-updated"]
   ["f1" tenant user "company-document-downloaded"]
   ["01" tenant user "company-document-updated"]
   ["02" tenant user "company-document-updated"]
   ["0b" other-tenant user "company-document-updated"]
   ["12" tenant user "company-document-updated"]
   ["15" tenant nil nil]])

(def ^:private expected-deleted
  "Worked out by hand from the rules above, so the check can't pass by
  both sides picking nothing."
  (set (map doc-id ["a1" "b2" "d1" "d4" "e1" "f1" "01" "0b" "0e" "11" "13" "16"])))

(defn- quoted [v] (if (nil? v) "NULL" (str "'" v "'")))

(defn- fixture-sql []
  (concat
   ["CREATE TYPE attachment_type AS ENUM ('sharepoint', 'upload')"
    "CREATE TABLE company_document (id uuid PRIMARY KEY, company_id uuid NOT NULL, tenant_id uuid NOT NULL,
       reference_id text, created_at timestamp NOT NULL, archived_at timestamp, deleted_at timestamp)"
    "CREATE TABLE attachment (id uuid PRIMARY KEY, type attachment_type NOT NULL)"
    "CREATE TABLE company_document_attachment (id serial PRIMARY KEY,
       company_document_id uuid NOT NULL REFERENCES company_document (id), attachment_id uuid NOT NULL REFERENCES attachment (id))"
    "CREATE TABLE activity (id serial PRIMARY KEY, \"tenantId\" uuid NOT NULL, \"userId\" uuid, \"type\" text)"
    "CREATE TABLE activity_resource (id serial PRIMARY KEY, \"activityId\" integer NOT NULL REFERENCES activity (id),
       \"resourceableId\" text NOT NULL, \"resourceableType\" text NOT NULL)"]
   (for [[code company tenant-id reference day archived? deleted?] documents]
     (format "INSERT INTO company_document VALUES ('%s', '%s', '%s', %s, '2026-10-%02d 10:00', %s, %s)"
             (doc-id code) company tenant-id (quoted reference) day
             (if archived? "'2026-10-20'" "NULL") (if deleted? "'2026-10-20'" "NULL")))
   (for [[code type] attachments]
     (format "INSERT INTO attachment VALUES ('%s', '%s')" (att-id code) type))
   (for [[doc att] links]
     (format "INSERT INTO company_document_attachment (company_document_id, attachment_id) VALUES ('%s', '%s')" (doc-id doc) (att-id att)))
   (mapcat (fn [i [doc tenant-id user-id type]]
             [(format "INSERT INTO activity (id, \"tenantId\", \"userId\", \"type\") VALUES (%d, '%s', %s, %s)"
                      (inc i) tenant-id (quoted user-id) (quoted type))
              (format "INSERT INTO activity_resource (\"activityId\", \"resourceableId\", \"resourceableType\") VALUES (%d, '%s', 'company-document')"
                      (inc i) (doc-id doc))])
           (range) activities)))

(def ^:private update-sql
  "The motivating script's statement that changes data."
  "WITH sp_docs AS (
    SELECT d.id, d.company_id, d.reference_id, d.created_at, d.archived_at,
        COALESCE(MIN(a.id::text) FILTER (WHERE a.type = 'sharepoint'), MIN(a.id::text)) AS any_attachment_id
    FROM company_document d
    LEFT JOIN company_document_attachment cda ON cda.company_document_id = d.id
    LEFT JOIN attachment a ON a.id = cda.attachment_id
    WHERE d.tenant_id = '86b8ad91-7463-478a-aa43-43a977105bf4' AND d.deleted_at IS NULL
    GROUP BY d.id, d.company_id, d.reference_id, d.created_at, d.archived_at
    HAVING BOOL_OR(a.type = 'sharepoint') OR d.reference_id IS NOT NULL
),
grouped AS (SELECT s.*, COALESCE(s.reference_id, 'att:' || s.any_attachment_id) AS dedupe_key FROM sp_docs s),
edited AS (
    SELECT DISTINCT g.id AS document_id FROM grouped g
    JOIN activity_resource ar ON ar.\"resourceableId\" = g.id::text
    JOIN activity act ON act.id = ar.\"activityId\"
    WHERE act.\"tenantId\" = '86b8ad91-7463-478a-aa43-43a977105bf4'
      AND act.\"userId\" IS DISTINCT FROM 'd3b93066-ea59-4259-a534-943249fba79c'
      AND act.\"type\" IS DISTINCT FROM 'company-document-downloaded'
),
ranked AS (
    SELECT g.*, (e.document_id IS NOT NULL) AS has_activity,
        ROW_NUMBER() OVER (PARTITION BY g.company_id, g.dedupe_key
            ORDER BY (e.document_id IS NOT NULL) DESC, CASE WHEN e.document_id IS NOT NULL THEN g.created_at END DESC, g.created_at DESC, g.id ASC) AS rn,
        ROW_NUMBER() OVER (PARTITION BY g.company_id, g.dedupe_key ORDER BY g.created_at ASC, g.id ASC) AS rn_oldest_only
    FROM grouped g LEFT JOIN edited e ON e.document_id = g.id WHERE g.archived_at IS NULL
),
keep_final AS (SELECT id, has_activity FROM ranked WHERE rn = 1)
UPDATE company_document d SET deleted_at = NOW()
FROM grouped g LEFT JOIN keep_final kf ON kf.id = g.id
WHERE d.id = g.id AND kf.id IS NULL AND g.archived_at IS NULL")

(def ^:private script
  (str "BEGIN;\n" update-sql ";\n"
       "SELECT COUNT(*) FROM company_document\n"
       "WHERE tenant_id = '86b8ad91-7463-478a-aa43-43a977105bf4' AND deleted_at >= NOW() - INTERVAL '1 minute';\n"
       "COMMIT;\n"))

(defn- spec
  "A clojure.java.jdbc spec for database `dbname` on the server."
  [^URI uri dbname]
  (let [[user password] (s/split (.getUserInfo uri) #":" 2)]
    {:connection-uri (str "jdbc:postgresql://" (.getHost uri) ":" (.getPort uri) "/" dbname
                          "?user=" user "&password=" password)
     :user user :password password :host (.getHost uri) :port (.getPort uri)}))

(defn- with-database
  "Run f with a fresh database, filled with the fixture, and drop it after."
  ([f] (with-database (fixture-sql) f))
  ([statements f]
   (let [uri (URI. server)
         admin (spec uri (subs (.getPath uri) 1))
         dbname (str "pine_equivalence_" (System/currentTimeMillis))]
     (jdbc/execute! admin [(str "CREATE DATABASE " dbname)] {:transaction? false})
     (try
       (let [db (spec uri dbname)]
         (doseq [statement statements]
           (jdbc/execute! db [statement]))
         (f db dbname))
       (finally
         (jdbc/execute! admin [(str "DROP DATABASE IF EXISTS " dbname " WITH (FORCE)")] {:transaction? false}))))))

(defn- sql-deleted
  "The ids the SQL's UPDATE changes, in a transaction that is rolled back."
  [db]
  (jdbc/with-db-transaction [tx db]
    (jdbc/db-set-rollback-only! tx)
    (set (map (comp str :id) (jdbc/query tx [(str update-sql " RETURNING d.id")])))))

(defn- pine-deleted
  "The ids the converted Pine picks, by running it without its update!:
  the rows a person sees before deciding to run it."
  [expression id]
  (let [blocks (s/split expression #"\n\s*\n")
        preview (s/replace (last blocks) #"\n \| update! .*$" "\n | select: id")
        result (api/api-eval (conj (vec (butlast blocks)) preview) id)]
    (is (nil? (:error result)) (str (:error result)))
    (set (map (comp str first) (rest (:result result))))))

(deftest test-motivating-script-on-postgres
  (if-not server
    (println "Skipping the Postgres equivalence check: set PINE_TEST_POSTGRES_URL to run it.")
    (with-database
      (fn [db dbname]
        (let [{:keys [host port user password]} db
              id (connections/add-connection-pool {:dbtype "postgres" :host host :port port :dbname dbname
                                                   :user user :password password})]
          (try
            (let [{:keys [expression warnings error]} (t/sql->pine script (db/init-references id))
                  from-sql (sql-deleted db)
                  from-pine (pine-deleted expression id)]
              (testing "The script converts in full"
                (is (nil? error) error)
                (is (= ["Left out BEGIN and COMMIT. Pine runs each update! and delete! in a transaction of its own."
                        "Left out the statement on line 32. Pine converts one statement at a time."
                        "Left out rn_oldest_only: a ROW_NUMBER() in `ranked` that no later query uses."]
                       warnings)))
              (testing "The fixture exercises the rules: the SQL deletes what they say"
                (is (= expected-deleted from-sql)))
              (testing "The Pine picks exactly the rows the SQL changes"
                (is (= from-sql from-pine)))
              (testing "Without the schema, the Pine guards against NULLs, and still picks the same rows"
                (let [{guarded :expression} (t/sql->pine script)]
                  (is (s/includes? guarded "if id in edited then true else false as has_activity"))
                  (is (= from-sql (pine-deleted guarded id)))))
              (testing "Run, the Pine's update! changes the same rows (the database is a throwaway)"
                (let [result (api/api-eval (s/split expression #"\n\s*\n") id)]
                  (is (nil? (:error result)) (str (:error result)))
                  (is (= from-sql (set (map (comp str :id) (jdbc/query db ["SELECT id FROM company_document WHERE deleted_at IS NOT NULL AND id <> ?::uuid" (doc-id "07")]))))))))
            (finally
              (connections/remove-connection-pool id)
              (db/clear-connection-if id))))))))

;; EXISTS and NOT EXISTS become `in` and `not in`, which differ from them on
;; NULLs. Users: 1 has a counterparty role, all archived. 2 has one that is
;; still active. 3 has no roles. 4 has an archived counterparty role and a
;; role with a NULL user next to it. 5 has no roles and a NULL key. 6 is
;; archived. `key` is a nullable copy of the id, so the outer column can be
;; NULL too; "userId" can be NULL on the inner side.
(def ^:private exists-fixture
  ["CREATE TABLE \"user\" (id int PRIMARY KEY, key int, user_type text NOT NULL, archived_at timestamptz)"
   "CREATE TABLE user_tenant_role (id int PRIMARY KEY, \"userId\" int, type text NOT NULL, deleted_at timestamptz, archived_at timestamptz)"
   "INSERT INTO \"user\" VALUES (1, 1, 'managed', NULL), (2, 2, 'managed', NULL), (3, 3, 'managed', NULL),
      (4, 4, 'managed', NULL), (5, NULL, 'managed', NULL), (6, 6, 'managed', now())"
   "INSERT INTO user_tenant_role VALUES
      (1, 1, 'counterparty', NULL, now()), (2, 1, 'counterparty_officer', now(), NULL),
      (3, 2, 'counterparty', NULL, NULL),
      (4, 4, 'counterparty', NULL, now()), (5, NULL, 'other', NULL, NULL),
      (6, 6, 'counterparty', NULL, now())"])

(def ^:private exists-queries
  "SELECTs of user ids. The first is the one reported: users whose roles
  are all archived or deleted. The key column may be NULL on both sides."
  ["SELECT u.id FROM \"user\" u WHERE u.user_type = 'managed' AND u.archived_at IS NULL
      AND EXISTS (SELECT 1 FROM user_tenant_role utr WHERE utr.\"userId\" = u.id AND utr.type IN ('counterparty', 'counterparty_officer'))
      AND NOT EXISTS (SELECT 1 FROM user_tenant_role utr WHERE utr.\"userId\" = u.id AND utr.deleted_at IS NULL AND utr.archived_at IS NULL)"
   "SELECT u.id FROM \"user\" u WHERE NOT EXISTS (SELECT 1 FROM user_tenant_role r WHERE r.\"userId\" = u.key)"
   "SELECT u.id FROM \"user\" u WHERE u.archived_at IS NOT NULL OR NOT EXISTS (SELECT 1 FROM user_tenant_role r WHERE u.key = r.\"userId\" AND r.archived_at IS NULL)"
   "SELECT u.id FROM \"user\" u WHERE NOT (u.id = 3 OR EXISTS (SELECT * FROM user_tenant_role r WHERE r.\"userId\" = u.key))"
   "WITH k AS (SELECT \"userId\" FROM user_tenant_role) SELECT u.id FROM \"user\" u WHERE NOT EXISTS (SELECT 1 FROM k WHERE k.\"userId\" = u.key)"])

(deftest test-exists-on-postgres
  (if-not server
    (println "Skipping the Postgres EXISTS check: set PINE_TEST_POSTGRES_URL to run it.")
    (with-database exists-fixture
      (fn [db dbname]
        (let [{:keys [host port user password]} db
              id (connections/add-connection-pool {:dbtype "postgres" :host host :port port :dbname dbname
                                                   :user user :password password})
              pine-ids (fn [expression]
                         (let [result (api/api-eval (s/split expression #"\n\s*\n") id)]
                           (is (nil? (:error result)) (str (:error result)))
                           (set (map first (rest (:result result))))))]
          (try
            (testing "The reported query keeps the users whose roles are all archived or deleted"
              (is (= #{1 4} (set (map :id (jdbc/query db [(first exists-queries)]))))))
            (doseq [sql exists-queries
                    references [(db/init-references id) nil]]
              (testing (str (if references "With" "Without") " the schema: " sql)
                (let [{:keys [expression error]} (t/sql->pine sql references)]
                  (is (nil? error) error)
                  (when expression
                    (is (= (set (map :id (jdbc/query db [sql]))) (pine-ids expression)) expression)))))
            (finally
              (connections/remove-connection-pool id)
              (db/clear-connection-if id))))))))

;; A JSON value read two ways, and NULLs sorted first. Templates of tenant
;; 'a': 1 and 2 have no company, 1 is an array, 2 has nodes, 3 has no nodes
;; key, 4 a string layoutVersion, 5 a numeric one. 6 belongs to tenant 'b'.
(def ^:private template-fixture
  ["CREATE TABLE company_structure_template (id int PRIMARY KEY, name text, company_id int, is_primary boolean,
      created_at timestamptz, updated_at timestamptz, tenant_id text NOT NULL, template jsonb)"
   "INSERT INTO company_structure_template VALUES
      (1, 'one', NULL, false, '2026-01-02', NULL, 'a', '[{\"id\": 1}, {\"id\": 2}, {\"id\": 3}]'),
      (2, 'two', NULL, true, '2026-01-03', NULL, 'a', '{\"nodes\": [1, 2], \"layoutVersion\": 2}'),
      (3, 'three', 10, false, '2026-01-01', NULL, 'a', '{\"layoutVersion\": \"v3\"}'),
      (4, 'four', 10, true, '2026-01-04', NULL, 'a', '{\"nodes\": [], \"layoutVersion\": \"v4\"}'),
      (5, 'five', 7, false, '2026-01-05', NULL, 'a', '{\"nodes\": [1], \"layoutVersion\": 5}'),
      (6, 'six', NULL, false, '2026-01-01', NULL, 'b', '[1]')"])

(def ^:private template-query
  "The query reported: it used to be refused for its ->, NULLS FIRST and
  jsonb functions."
  "SELECT t.id, t.name, t.company_id, t.is_primary, t.created_at, t.updated_at,
       jsonb_array_length(CASE WHEN jsonb_typeof(t.template)='array'
            THEN t.template ELSE t.template->'nodes' END) AS nodes,
       t.template->'layoutVersion' AS lv
FROM company_structure_template t
WHERE t.tenant_id = 'a'
ORDER BY t.company_id NULLS FIRST, t.is_primary DESC, t.created_at;")

(deftest test-json-and-nulls-first-on-postgres
  (if-not server
    (println "Skipping the Postgres JSON check: set PINE_TEST_POSTGRES_URL to run it.")
    (with-database template-fixture
      (fn [db dbname]
        (let [{:keys [host port user password]} db
              id (connections/add-connection-pool {:dbtype "postgres" :host host :port port :dbname dbname
                                                   :user user :password password})]
          (try
            (doseq [references [(db/init-references id) nil]]
              (testing (str (if references "With" "Without") " the schema")
                (let [{:keys [expression error warnings]} (t/sql->pine template-query references)
                      result (when expression (api/api-eval [expression] id))
                      [header & rows] (:result result)
                      at (fn [k] (.indexOf ^java.util.List (vec header) k))
                      pine (map (fn [r] [(nth r (at :id)) (nth r (at :nodes)) (nth r (at :lv))]) rows)
                      ;; `->` gives JSON: the string "v3" comes back quoted. Pine
                      ;; shows it as text, as the warning says.
                      from-sql (map (fn [{:keys [id nodes lv]}]
                                      [id nodes (some-> lv str json/parse-string str)])
                                    (jdbc/query db [template-query]))]
                  (is (nil? error) error)
                  (is (= ["In SQL, `->` gives JSON, so a string keeps its quotes. Pine shows template.layoutVersion as text, without them, as `->>` does."]
                         warnings))
                  (is (nil? (:error result)) (str (:error result)))
                  (testing "Same rows, in the same order, NULL companies first"
                    (is (= [2 1 5 4 3] (map first from-sql)))
                    (is (= from-sql pine) expression)))))
            (testing "ORDER BY a -> key sorts JSON in both: numbers above strings, NULLs where asked"
              (let [sql "SELECT t.id, t.template->'layoutVersion' AS lv FROM company_structure_template t
                         WHERE t.tenant_id = 'a' ORDER BY lv DESC NULLS LAST, t.id"
                    {:keys [expression error]} (t/sql->pine sql (db/init-references id))
                    result (api/api-eval [expression] id)
                    [header & rows] (:result result)]
                (is (nil? error) error)
                (is (nil? (:error result)) (str (:error result)))
                (is (= [5 2 4 3 1] (map :id (jdbc/query db [sql]))))
                (is (= (map :id (jdbc/query db [sql]))
                       (map #(nth % (.indexOf ^java.util.List (vec header) :id)) rows))
                    expression)))
            (finally
              (connections/remove-connection-pool id)
              (db/clear-connection-if id))))))))
