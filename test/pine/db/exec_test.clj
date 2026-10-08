(ns pine.db.exec-test
  (:require [clojure.test :refer [deftest is testing]]
            [pine.db.exec :as exec]))

(defn- stopped-error-type
  "Starts a statement under run-id and returns the error-type it fails with.
  A stopped run fails before it touches the connection, so none is needed."
  [run-id]
  (binding [exec/*run-id* run-id]
    (try (#'exec/run-statement nil "SELECT 1" {} (fn [_] :ran))
         (catch clojure.lang.ExceptionInfo e (:error-type (ex-data e))))))

(deftest test-cancel
  (testing "a stop that arrives before the run's statement still stops it"
    (is (false? (exec/cancel! "exec-test-1")) "nothing was running yet")
    (is (= "cancelled" (stopped-error-type "exec-test-1")))
    (exec/finish-run! "exec-test-1"))

  (testing "a run id is forgotten once its run finishes"
    (exec/cancel! "exec-test-2")
    (exec/finish-run! "exec-test-2")
    (is (= :ran (binding [exec/*run-id* "exec-test-2"]
                  (with-redefs [clojure.java.jdbc/prepare-statement (fn [& _] (reify java.lang.AutoCloseable (close [_])))]
                    (#'exec/run-statement nil "SELECT 1" {} (fn [_] :ran)))))))

  (testing "stopping one run leaves another alone"
    (exec/cancel! "exec-test-3")
    (is (= :ran (binding [exec/*run-id* "exec-test-4"]
                  (with-redefs [clojure.java.jdbc/prepare-statement (fn [& _] (reify java.lang.AutoCloseable (close [_])))]
                    (#'exec/run-statement nil "SELECT 1" {} (fn [_] :ran))))))
    (exec/finish-run! "exec-test-3")))
