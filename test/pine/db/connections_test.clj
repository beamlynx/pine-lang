(ns pine.db.connections-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [pine.db.connections :as connections]
   [pine.db.main :as db])
  (:import (com.zaxxer.hikari HikariDataSource)))

(defn- fake-pool [closed?]
  (reify java.io.Closeable
    (close [_] (reset! closed? true))))

(deftest test-remove-connection-pool
  (testing "removes an existing pool and closes it"
    (let [closed? (atom false)]
      (swap! connections/pools assoc "conn-remove-1" (fake-pool closed?))
      (connections/remove-connection-pool "conn-remove-1")
      (is @closed? "pool should be closed")
      (is (not (contains? @connections/pools "conn-remove-1")))))

  (testing "throws for an unknown id"
    (is (thrown? Exception (connections/remove-connection-pool "conn-does-not-exist")))))

(deftest test-clear-connection-if
  (let [original @db/connection-id]
    (try
      (testing "clears the selected connection when it matches the removed id"
        (reset! db/connection-id "conn-a")
        (db/clear-connection-if "conn-a")
        (is (nil? @db/connection-id)))

      (testing "leaves a different selected connection untouched"
        (reset! db/connection-id "conn-b")
        (db/clear-connection-if "conn-a")
        (is (= "conn-b" @db/connection-id)))
      (finally
        (reset! db/connection-id original)))))

(deftest test-reindex-references
  (testing "re-runs the indexer even when a value is already cached"
    (let [calls (atom 0)]
      (with-redefs [db/get-indexed-references (fn [_id] (swap! calls inc) {:call @calls})]
        (is (= "conn-reindex" (db/reindex-references "conn-reindex")))
        (is (= {:call 1} (@db/references "conn-reindex")))
        (db/reindex-references "conn-reindex")
        (is (= {:call 2} (@db/references "conn-reindex")))))))

;; A real HikariDataSource subclass, not a reify: add-connection-pool has to
;; distinguish a realized pool from the lazy thunk get-connection-pool also
;; accepts, so it checks `instance? HikariDataSource`. The no-arg
;; constructor is the "configure later" mode and starts no pool, so nothing
;; here touches a database.
(defn- fake-hikari [url user closed?]
  (proxy [HikariDataSource] []
    (getJdbcUrl [] url)
    (getUsername [] user)
    (close [] (reset! closed? true))))

(deftest test-add-connection-pool-does-not-leak
  (testing "re-registering the same database reuses the pool instead of stacking a new one"
    ;; The regression this guards: the id is derived from the pool's own
    ;; host:port:dbname, so this always lands on the same key. A bare
    ;; `swap! pools assoc` overwrote the entry and left the previous
    ;; HikariDataSource open and unreferenced -- and since the pool config
    ;; sets minimumIdle 1, each orphan held a real Postgres connection for
    ;; the life of the process.
    (let [first-closed? (atom false)
          second-closed? (atom false)
          url "jdbc:postgresql://leak-test:5432/app"]
      (try
        (with-redefs [connections/create-pool (fn [_] (fake-hikari url "app_user" first-closed?))]
          (is (= "leak-test:5432:app" (connections/add-connection-pool {:host "leak-test"}))))
        (with-redefs [connections/create-pool (fn [_] (fake-hikari url "app_user" second-closed?))]
          (is (= "leak-test:5432:app" (connections/add-connection-pool {:host "leak-test"}))))

        (is (not @first-closed?) "the pool still in the registry must stay open")
        (is @second-closed? "the redundant second pool must be closed, not leaked")
        (is (= 1 (count (filter #(= "leak-test:5432:app" %) (keys @connections/pools)))))
        (finally
          (swap! connections/pools dissoc "leak-test:5432:app")))))

  (testing "a different database on the same host:port registers as a separate pool"
    ;; The id now folds dbname in (host:port:dbname), so two databases on one
    ;; server land on distinct keys and can coexist -- this is the whole
    ;; point of the multi-database-per-server feature. Both stay open; there
    ;; is nothing here to reject or close.
    (let [one-closed? (atom false)
          two-closed? (atom false)]
      (try
        (with-redefs [connections/create-pool
                      (fn [_] (fake-hikari "jdbc:postgresql://swap-test:5432/one" "u1" one-closed?))]
          (is (= "swap-test:5432:one" (connections/add-connection-pool {:host "swap-test"}))))
        (with-redefs [connections/create-pool
                      (fn [_] (fake-hikari "jdbc:postgresql://swap-test:5432/two" "u1" two-closed?))]
          (is (= "swap-test:5432:two" (connections/add-connection-pool {:host "swap-test"}))))

        (is (not @one-closed?) "the first database's pool must stay open")
        (is (not @two-closed?) "the second database's pool must stay open too")
        (is (= "jdbc:postgresql://swap-test:5432/one"
               (.getJdbcUrl (@connections/pools "swap-test:5432:one"))))
        (is (= "jdbc:postgresql://swap-test:5432/two"
               (.getJdbcUrl (@connections/pools "swap-test:5432:two"))))
        (finally
          (swap! connections/pools dissoc "swap-test:5432:one" "swap-test:5432:two")))))

  (testing "a different user on the same database is rejected"
    (let [old-closed? (atom false)
          new-closed? (atom false)
          url "jdbc:postgresql://user-test:5432/app"]
      (try
        (with-redefs [connections/create-pool (fn [_] (fake-hikari url "reader" old-closed?))]
          (connections/add-connection-pool {:host "user-test"}))
        (with-redefs [connections/create-pool (fn [_] (fake-hikari url "writer" new-closed?))]
          (is (thrown-with-msg? clojure.lang.ExceptionInfo #"already registered"
                                (connections/add-connection-pool {:host "user-test"}))))

        (is (not @old-closed?) "credentials differing must not silently take over the existing pool")
        (is @new-closed? "the rejected pool must be closed, not leaked")
        (finally
          (swap! connections/pools dissoc "user-test:5432:app"))))))

(deftest test-jdbc-url
  (testing "Postgres: dbtype defaults, port defaults to 5432"
    (is (= "jdbc:postgresql://h:5432/d" (connections/jdbc-url {:host "h" :dbname "d"})))
    (is (= "jdbc:postgresql://h:5433/d" (connections/jdbc-url {:host "h" :port 5433 :dbname "d"}))))

  (testing "MySQL: port defaults to 3306, plus the fixed connection params"
    (is (= (str "jdbc:mysql://h:3306/d?connectionTimeZone=SERVER&forceConnectionTimeZoneToSession=false"
                "&preserveInstants=false&allowPublicKeyRetrieval=true"
                "&zeroDateTimeBehavior=CONVERT_TO_NULL&tinyInt1isBit=false&characterEncoding=UTF-8")
           (connections/jdbc-url {:dbtype "mysql" :host "h" :dbname "d"})))
    (is (clojure.string/starts-with? (connections/jdbc-url {:dbtype "mysql" :host "h" :port 3307 :dbname "d"})
                                     "jdbc:mysql://h:3307/d?"))))

(deftest test-jdbc-url->label
  (testing "Postgres URL, no query string"
    (is (= "h:5432 · d" (connections/jdbc-url->label "jdbc:postgresql://h:5432/d"))))

  (testing "MySQL URL with a ?params suffix - regression test for the naive (s/split url #\"/\") parse, which took the ?params-corrupted segment as the dbname"
    (is (= "h:3306 · pine"
           (connections/jdbc-url->label
            (connections/jdbc-url {:dbtype "mysql" :host "h" :dbname "pine"}))))))

(deftest test-get-dialect
  (testing "sentinel test connection ids resolve without touching any pool"
    (is (= :postgres (connections/get-dialect :test)))
    (is (= :mysql (connections/get-dialect :test-mysql))))

  (testing "a registered HikariDataSource's own JDBC URL scheme wins"
    (try
      (swap! connections/pools assoc "dialect-mysql" (fake-hikari "jdbc:mysql://h:3306/d" "u" (atom false)))
      (swap! connections/pools assoc "dialect-pg" (fake-hikari "jdbc:postgresql://h:5432/d" "u" (atom false)))
      (is (= :mysql (connections/get-dialect "dialect-mysql")))
      (is (= :postgres (connections/get-dialect "dialect-pg")))
      (finally
        (swap! connections/pools dissoc "dialect-mysql" "dialect-pg"))))

  (testing "defaults to :postgres for anything else - an unregistered id, or a non-HikariDataSource pool"
    (is (= :postgres (connections/get-dialect "no-such-connection")))
    (try
      (swap! connections/pools assoc "dialect-fake" (fake-pool (atom false)))
      (is (= :postgres (connections/get-dialect "dialect-fake")))
      (finally
        (swap! connections/pools dissoc "dialect-fake")))))
