(ns pine.db.policy-test
  "The masking policy on real SQLite, MySQL and Postgres databases: a masked
  column's contents never come back, and a query may still filter on it.
  How SQLite's own types and case meet the policy is in
  pine.db.engines.sqlite-test. See pine.db.engines."
  (:require [clojure.test :refer [deftest is testing]]
            [pine.db.engines :as e]))

(def ^:private fixture
  ["CREATE TABLE customer (id INTEGER PRIMARY KEY, name {text}, email {text}, data {json})"
   (str "INSERT INTO customer VALUES (1, 'Ann', 'ann@example.com', '{\"ssn\": \"123\"}'), "
        "(2, 'Bob', 'bob@example.com', '{\"ssn\": \"456\"}')")])

(def ^:private hide-email-and-data
  [{:type "column-name" :suffix "name"}
   {:type "hide" :match {:type "column" :table "customer" :column "email"}}])

(defn- masked [db expression]
  (let [{:keys [rows columns error]} (e/pine db expression {:policy hide-email-and-data})
        shown (keep-indexed (fn [i c] (when-not (:hidden c) i)) columns)]
    {:error error :rows (mapv (fn [row] (mapv #(nth row %) shown)) rows)}))

(deftest test-masking
  (e/on-every-engine
   fixture
   (fn [db]
     (testing "a masked column comes back as xxxxx, and one the policy shows as it is"
       (is (= {:error nil :rows [["Ann" "xxxxx"] ["Bob" "xxxxx"]]}
              (masked db "customer | s: name, email | o: id asc"))))

     (testing "a key inside a masked JSON column is masked too"
       (is (= [["xxxxx"] ["xxxxx"]] (:rows (masked db "customer | s: data.ssn | o: id asc")))))

     (testing "a query may filter on a masked column with a value it already has"
       (is (= {:error nil :rows [["Bob"]]}
              (masked db "customer | where: email = 'bob@example.com' | s: name"))))

     (testing "but not group on it, which would return its values"
       (is (re-find #"masked column" (str (:error (masked db "customer | group: email with count")))))))))
