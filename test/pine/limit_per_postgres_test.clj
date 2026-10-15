(ns pine.limit-per-postgres-test
  "`limit: N per`, run on a real Postgres: which rows it keeps, and the order
  they come back in. The SQL tests in limit_per_test.clj check the query's
  text. This checks what the database does with it.

  Runs only when PINE_TEST_POSTGRES_URL names a Postgres server, like
  postgres://pine:pine@localhost:5435/pine. The user must be allowed to
  create a database: the test creates one with a fresh name, fills it, and
  drops it at the end. Nothing else on the server is touched."
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as s]
            [clojure.test :refer [deftest is testing]]
            [pine.api :as api]
            [pine.db.connections :as connections]
            [pine.db.main :as db])
  (:import (java.net URI)))

(def ^:private server (System/getenv "PINE_TEST_POSTGRES_URL"))

;; Five logins and one logout, across two tenants:
;;
;;   08:00  Acme  logout
;;   09:00  Acme  login
;;   09:05  Beta  login
;;   09:10  Acme  login
;;   09:15  Acme  login
;;   09:20  Beta  login
(def ^:private fixture
  ["CREATE TABLE tenant (id int PRIMARY KEY, title text NOT NULL)"
   "CREATE TABLE activity (id int PRIMARY KEY, tenant_id int NOT NULL REFERENCES tenant (id), type text NOT NULL, created_at timestamp NOT NULL)"
   "INSERT INTO tenant VALUES (1, 'Acme'), (2, 'Beta')"
   (str "INSERT INTO activity VALUES "
        "(1, 1, 'logout', '2026-10-15 08:00'), (2, 1, 'login', '2026-10-15 09:00'), (3, 2, 'login', '2026-10-15 09:05'), "
        "(4, 1, 'login', '2026-10-15 09:10'), (5, 1, 'login', '2026-10-15 09:15'), (6, 2, 'login', '2026-10-15 09:20')")])

(defn- spec [^URI uri dbname]
  (let [[user password] (s/split (.getUserInfo uri) #":" 2)]
    {:connection-uri (str "jdbc:postgresql://" (.getHost uri) ":" (.getPort uri) "/" dbname
                          "?user=" user "&password=" password)
     :user user :password password :host (.getHost uri) :port (.getPort uri)}))

(defn- with-connection
  "Run f with a Pine connection id for a fresh database holding the fixture.
  Drops the database after."
  [f]
  (let [uri (URI. server)
        admin (spec uri (subs (.getPath uri) 1))
        dbname (str "pine_limit_per_" (System/currentTimeMillis))]
    (jdbc/execute! admin [(str "CREATE DATABASE " dbname)] {:transaction? false})
    (try
      (let [{:keys [host port user password] :as database} (spec uri dbname)]
        (doseq [statement fixture] (jdbc/execute! database [statement]))
        (let [id (connections/add-connection-pool {:dbtype "postgres" :host host :port port :dbname dbname
                                                   :user user :password password})]
          (try
            (db/init-references id)
            (f id)
            (finally
              (connections/remove-connection-pool id)
              (db/clear-connection-if id)))))
      (finally
        (jdbc/execute! admin [(str "DROP DATABASE IF EXISTS " dbname " WITH (FORCE)")] {:transaction? false})))))

(defn- rows
  "The rows the expression returns, as [title \"HH:MM\"]."
  [id expression]
  (let [{:keys [result error]} (api/api-eval [expression] id)]
    (is (nil? error) (str error))
    (map (fn [[title created-at]] [title (subs (str created-at) 11 16)]) (rest result))))

(def ^:private logins
  "activity as a | where: a.type = 'login' | tenant as t | from: a | select: t.title, a.created_at")

(deftest test-limit-per-on-postgres
  (if-not server
    (println "Skipping the limit: ... per check on Postgres: set PINE_TEST_POSTGRES_URL to run it.")
    (with-connection
      (fn [id]
        (testing "the order: before per picks each group's first rows, and the result comes group by group"
          (is (= [["Acme" "09:00"] ["Acme" "09:10"] ["Beta" "09:05"] ["Beta" "09:20"]]
                 (rows id (str logins " | order: a.created_at asc | limit: 2 per a.tenant_id")))))

        (testing "the other way round, the latest rows of each group"
          (is (= [["Acme" "09:15"] ["Acme" "09:10"] ["Beta" "09:20"] ["Beta" "09:05"]]
                 (rows id (str logins " | order: a.created_at desc | limit: 2 per a.tenant_id")))))

        (testing "an order: after it sorts the whole result again"
          (is (= [["Acme" "09:00"] ["Beta" "09:05"] ["Acme" "09:10"] ["Beta" "09:20"]]
                 (rows id (str logins " | order: a.created_at asc | limit: 2 per a.tenant_id | order: created_at asc")))))))))
