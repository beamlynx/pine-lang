(ns pine.similar-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [pine.similar :refer [close-names did-you-mean]]))

(deftest test-close-names
  (testing "case and underscores are ignored, and such a match is all that is returned"
    (is (= ["tenantId"] (close-names "tenant_id" ["id" "tenantId" "tenant" "parent_id"])))
    (is (= ["user_id"] (close-names "USERID" ["user_id" "users_id"]))))

  (testing "otherwise up to two characters apart, best first"
    (is (= ["user_id" "users_id"] (close-names "usr_id" ["users_id" "user_id" "id"])))
    (is (= [] (close-names "nickname" ["id" "title" "created_at"]))))

  (testing "a short name may be only one character apart"
    (is (= ["name"] (close-names "nme" ["name" "id"])))
    (is (= [] (close-names "ab" ["id"]))))

  (testing "at most three"
    (is (= 3 (count (close-names "col" ["col1" "col2" "col3" "col4"]))))))

(deftest test-did-you-mean
  (is (= "Did you mean `tenantId`?" (did-you-mean "tenant_id" ["tenantId"])))
  (is (= "Did you mean `col1`, `col2` or `col3`?" (did-you-mean "col" ["col1" "col2" "col3"])))
  (is (nil? (did-you-mean "nickname" ["id"]))))
