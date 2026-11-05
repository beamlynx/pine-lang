(ns pine.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [pine.api :as api]
            [pine.core :as core]))

(deftest test-ready-line
  (testing "beamlynx-desktop reads the version from the ready line and compares it with /api/v1/connections"
    (let [[_ v] (re-find #"^pine-server ready (\S+)" (core/ready-line))]
      (is (= "pine-server ready " (subs (core/ready-line) 0 18)))
      (is (= v (get-in (api/get-connections) [:result :version])))
      (is (= (core/ready-line) (str "pine-server ready " v)) "nothing after the version"))))
