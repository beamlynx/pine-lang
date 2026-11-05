(ns pine.parse-cache-test
  "A build reads the same text several times. Instaparse runs once per
  distinct text, and a block that hasn't changed isn't read again."
  (:require [clojure.test :refer [deftest is testing]]
            [pine.api :as api]
            [pine.parser :as parser]))

(def ^:private context
  ["company as c | employee | where: id = 1 | s: id, company_id |= a"
   "a | document .employee_id | where: id > 3 | o: id desc | l: 10 |= b"
   "b | s: id | where: id != 2 |= d"])

(defn- last-block [n]
  (str "company | employee | where: id = " n " or id = 2 | s: id | o: id desc | l: 5"))

(defn- build [expressions]
  (api/api-build expressions {:line 0 :character (count (last expressions))} :test))

(defn- counting-instaparse [calls]
  (let [p @#'parser/parser]
    (fn [& args] (swap! calls inc) (apply p args))))

(deftest test-unchanged-blocks-are-not-parsed-again
  (let [calls (atom 0)]
    (with-redefs [parser/parser (counting-instaparse calls)]
      (testing "the first build reads each block once, however often it uses it"
        (build (conj context (last-block 1001)))
        (is (<= @calls 4)))
      (testing "a keystroke in the last block reads only that block"
        (reset! calls 0)
        (build (conj context (last-block 1002)))
        (is (= 1 @calls)))
      (testing "a build of the same text reads nothing"
        (reset! calls 0)
        (build (conj context (last-block 1002)))
        (is (= 0 @calls))))))

(deftest test-cached-results-match-the-text
  (testing "each text gets its own result, a failure included"
    (let [texts ["company | s: id" "company | s: id " "company | s: name" "company | where: = 1" "company | where: = 1"]]
      (doseq [t texts]
        (is (= (parser/parse t) (parser/parse t)) t))
      (is (not= (parser/parse "company | s: id") (parser/parse "company | s: name")))
      (is (:error (parser/parse "company | where: = 1")))
      (is (= (:result (parser/prettify "company | s: id|l: 1"))
             "company\n | s: id\n | l: 1")))))
