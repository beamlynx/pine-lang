(ns pine.db.limit-per-test
  "`limit: N per`, on real SQLite, MySQL and Postgres databases: which rows
  it keeps, and the order they come back in. The SQL tests in
  pine.limit-per-test check the query's text. This checks what each
  database does with it. See pine.db.engines."
  (:require [clojure.test :refer [deftest is testing]]
            [pine.db.engines :as e]))

;; Five logins and one logout, across two tenants:
;;
;;   08:00  Acme  logout
;;   09:00  Acme  login
;;   09:05  Beta  login
;;   09:10  Acme  login
;;   09:15  Acme  login
;;   09:20  Beta  login
(def ^:private fixture
  ["CREATE TABLE tenant (id INTEGER PRIMARY KEY, title {text} NOT NULL)"
   (str "CREATE TABLE activity (id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL, type {text} NOT NULL, "
        "created_at {timestamp} NOT NULL, FOREIGN KEY (tenant_id) REFERENCES tenant (id))")
   "INSERT INTO tenant VALUES (1, 'Acme'), (2, 'Beta')"
   (str "INSERT INTO activity VALUES "
        "(1, 1, 'logout', '2026-10-15 08:00:00'), (2, 1, 'login', '2026-10-15 09:00:00'), (3, 2, 'login', '2026-10-15 09:05:00'), "
        "(4, 1, 'login', '2026-10-15 09:10:00'), (5, 1, 'login', '2026-10-15 09:15:00'), (6, 2, 'login', '2026-10-15 09:20:00')")])

(defn- logins
  "The rows of `expression`, as [title \"HH:MM\"]."
  [db expression]
  (mapv (fn [[title created-at]] [title (subs (str created-at) 11 16)]) (e/rows db expression)))

(def ^:private base
  "activity as a | where: a.type = 'login' | tenant as t | from: a | select: t.title, a.created_at")

(deftest test-limit-per
  (e/on-every-engine
   fixture
   (fn [db]
     (testing "the order: before per picks each group's first rows, and the result comes group by group"
       (is (= [["Acme" "09:00"] ["Acme" "09:10"] ["Beta" "09:05"] ["Beta" "09:20"]]
              (logins db (str base " | order: a.created_at asc | limit: 2 per a.tenant_id")))))

     (testing "the other way round, the latest rows of each group"
       (is (= [["Acme" "09:15"] ["Acme" "09:10"] ["Beta" "09:20"] ["Beta" "09:05"]]
              (logins db (str base " | order: a.created_at desc | limit: 2 per a.tenant_id")))))

     (testing "an order: after it sorts the whole result again"
       (is (= [["Acme" "09:00"] ["Beta" "09:05"] ["Acme" "09:10"] ["Beta" "09:20"]]
              (logins db (str base " | order: a.created_at asc | limit: 2 per a.tenant_id | order: created_at asc"))))))))
