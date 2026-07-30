(ns pine.db.connections-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [pine.db.connections :as connections]
   [pine.db.main :as db]))

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
