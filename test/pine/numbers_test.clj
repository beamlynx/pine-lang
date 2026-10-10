(ns pine.numbers-test
  "Numbers in where: and update!: whole, negative and decimal."
  (:require [clojure.test :refer [deftest is testing]]
            [pine.ast.main :as ast]
            [pine.eval :as eval]
            [pine.parser :as parser]))

(defn- parse [expression]
  (parser/parse expression))

(defn- sql [expression]
  (-> (:result (parse expression))
      (ast/generate :test nil nil {} [])
      eval/build-query
      (select-keys [:query :params])
      (update :params #(map :value %))))

(deftest test-parse
  (testing "A number can be negative or have a decimal part"
    (is (= [{:type :number :value -2} {:type :number :value 1.5M} {:type :number :value -0.25M}]
           (->> ["where: age = -2" "where: age = 1.5" "where: age > -0.25"]
                (map #(-> % parse :result first :value (nth 2)))))))

  (testing "A decimal is kept exact"
    (is (= 0.1M (-> "where: age = 0.1" parse :result first :value (nth 2) :value))))

  (testing "update! takes one too"
    (is (= {:type :number :value -1.5M}
           (-> "update! age = -1.5" parse :result first :value :assignments first :value))))

  (testing "limit: and an array index stay whole numbers of zero or more"
    (is (:error (parse "limit: -1")))
    (is (:error (parse "limit: 1.5")))
    (is (:error (parse "s: data[-1]")))
    (is (:error (parse "s: data[1.5]"))))

  (testing "A number too large to hold says to write it as a string"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"too large"
                          (parse "where: age = 99999999999999999999")))))

(deftest test-sql
  (testing "Numbers are bound as parameters"
    (is (= {:query "SELECT \"c_0\".\"id\" AS \"__c_0__id\", \"c_0\".* FROM \"customer\" AS \"c_0\" WHERE \"c_0\".\"id\" > ? LIMIT 250"
            :params [-3]}
           (sql "customer | where: id > -3")))))
