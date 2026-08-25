(ns pine.db.connections-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [pine.db.connections :as connections]
   [pine.db.main :as db]
   [pine.db.postgres :as postgres])
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
      (with-redefs [postgres/get-indexed-references (fn [_id] (swap! calls inc) {:call @calls})]
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
    ;; host:port, so this always lands on the same key. A bare
    ;; `swap! pools assoc` overwrote the entry and left the previous
    ;; HikariDataSource open and unreferenced -- and since the pool config
    ;; sets minimumIdle 1, each orphan held a real Postgres connection for
    ;; the life of the process.
    (let [first-closed? (atom false)
          second-closed? (atom false)
          url "jdbc:postgresql://leak-test:5432/app"]
      (try
        (with-redefs [connections/create-pool (fn [_] (fake-hikari url "app_user" first-closed?))]
          (is (= "leak-test:5432" (connections/add-connection-pool {:host "leak-test"}))))
        (with-redefs [connections/create-pool (fn [_] (fake-hikari url "app_user" second-closed?))]
          (is (= "leak-test:5432" (connections/add-connection-pool {:host "leak-test"}))))

        (is (not @first-closed?) "the pool still in the registry must stay open")
        (is @second-closed? "the redundant second pool must be closed, not leaked")
        (is (= 1 (count (filter #(= "leak-test:5432" %) (keys @connections/pools)))))
        (finally
          (swap! connections/pools dissoc "leak-test:5432")))))

  (testing "a different database on the same host:port is rejected, leaving the existing pool untouched"
    ;; A connection id is only host:port, so two different databases on one
    ;; server collide on the same key and could never coexist here. The old
    ;; code silently overwrote the entry, which pointed an existing
    ;; connection id at a different database -- a correctness hazard on top
    ;; of the leak. Closing the displaced pool instead would abort queries
    ;; still running against a database the user may still be using, so this
    ;; refuses rather than disturbing anything already registered.
    (let [old-closed? (atom false)
          new-closed? (atom false)]
      (try
        (with-redefs [connections/create-pool
                      (fn [_] (fake-hikari "jdbc:postgresql://swap-test:5432/one" "u1" old-closed?))]
          (connections/add-connection-pool {:host "swap-test"}))
        (with-redefs [connections/create-pool
                      (fn [_] (fake-hikari "jdbc:postgresql://swap-test:5432/two" "u1" new-closed?))]
          (is (thrown-with-msg? clojure.lang.ExceptionInfo #"already in use"
                                (connections/add-connection-pool {:host "swap-test"}))))

        (is (not @old-closed?) "the registered pool must not be closed out from under in-flight queries")
        (is @new-closed? "the rejected pool must be closed, not leaked")
        (is (= "jdbc:postgresql://swap-test:5432/one"
               (.getJdbcUrl (@connections/pools "swap-test:5432")))
            "the original registration must still be the one in the map")
        (finally
          (swap! connections/pools dissoc "swap-test:5432")))))

  (testing "a different user on the same database is rejected too"
    (let [old-closed? (atom false)
          new-closed? (atom false)
          url "jdbc:postgresql://user-test:5432/app"]
      (try
        (with-redefs [connections/create-pool (fn [_] (fake-hikari url "reader" old-closed?))]
          (connections/add-connection-pool {:host "user-test"}))
        (with-redefs [connections/create-pool (fn [_] (fake-hikari url "writer" new-closed?))]
          (is (thrown-with-msg? clojure.lang.ExceptionInfo #"already in use"
                                (connections/add-connection-pool {:host "user-test"}))))

        (is (not @old-closed?) "credentials differing must not silently take over the existing pool")
        (is @new-closed? "the rejected pool must be closed, not leaked")
        (finally
          (swap! connections/pools dissoc "user-test:5432"))))))
