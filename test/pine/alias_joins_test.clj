(ns pine.alias-joins-test
  "A join from a name given with `as`: `s: resourceId as company_id | company`.
  See docs/joins.md, \"Joining on a name you give\"."
  (:require [clojure.test :refer [deftest is testing]]
            [pine.ast.main :as ast]
            [pine.eval :as eval]
            [pine.parser :as parser]))

(defn- state
  ([expression] (state expression []))
  ([expression policy]
   (-> expression parser/parse :result (ast/generate :test nil nil {} policy))))

(defn- on-clause [expression]
  (second (re-find #" ON (.*?) LIMIT" (:query (eval/build-query (state expression))))))

(defn- join [expression]
  (-> (state expression) :joins last (select-keys [:from :to :columns :resolution :cast])))

(deftest test-join-from-a-name
  (testing "A column named `company_id` with `as` joins `company` on its `id`"
    (is (= {:from "c_0" :to "c_1" :columns [{:from "id" :to "id"}] :resolution "alias" :cast nil}
           (join "customer | s: id as company_id | company"))))

  (testing "camelCase and a plural table name work as in the index"
    (is (= "alias" (:resolution (join "customer | s: id as companyId | company")))))

  (testing "Two types that don't compare are cast to text, as a heuristic join is"
    (is (= "\"c_0\".\"uuid_col\"::text = \"u_1\".\"id\"::text"
           (on-clause "customer | s: uuid_col as user_id | user"))))

  (testing "A key inside JSON joins on its text"
    (is (= "jsonb_extract_path_text(\"c_0\".\"data\"::jsonb, ?::text) = \"e_1\".\"id\"::text"
           (on-clause "customer | s: data.ownerId as employee_id | employee"))))

  (testing "Without `as`, nothing changes"
    (is (nil? (:resolution (join "customer | s: uuid_col | user")))))

  (testing "A join column named with `.col` picks a relation of the index instead"
    (is (= "fk" (:resolution (join "company | s: id as employee_id | employee .company_id")))))

  (testing "The newest name wins"
    (is (= [{:from "uuid_col" :to "id"}]
           (:columns (join "customer | s: id as company_id | s: uuid_col as company_id | company")))))

  (testing "A name on another table than the one the join starts from doesn't count"
    (is (nil? (:resolution (join "customer | s: id as company_id | user .id = .id | company"))))))

(deftest test-join-from-an-unnest
  (testing "Elements named `company_id` join `company` on the element itself"
    (is (= {:from "company_id" :to "c_2" :columns [{:from "value" :to "id" :from-value-path []}]
            :resolution "alias" :cast nil}
           (join "customer | unnest: data as company_id | company")))
    (is (= "(\"company_id\".\"value\"::jsonb #>> '{}') = \"c_2\".\"id\"::text"
           (on-clause "customer | unnest: data as company_id | company"))))

  (testing "Elements named after the table join it on the element's `id`"
    (is (= [{:from "value" :to "id" :from-value-path ["id"]}]
           (:columns (join "customer | unnest: data.companies as company | company")))))

  (testing "A key of an element, named with `as`, joins too"
    (is (= {:from "e" :to "c_2" :columns [{:from "value" :to "id" :from-value-path ["companyId"]}]
            :resolution "alias" :cast nil}
           (join "customer | unnest: data as e | s: e.companyId as company_id | company")))))

(deftest test-join-from-a-name-hints
  (testing "The table a name leads to is suggested, as a bare table"
    (is (some #(= {:table "company" :resolution "alias" :pine "company"} (select-keys % [:table :resolution :pine]))
              (get-in (state "customer | s: uuid_col as company_id | ") [:hints :table])))
    (is (= [{:table "company" :resolution "alias" :pine "company"}]
           (map #(select-keys % [:table :resolution :pine])
                (get-in (state "customer | unnest: data.companies as company | comp") [:hints :table])))))

  (testing "No name, no suggestion"
    (is (not-any? #(= "alias" (:resolution %))
                  (get-in (state "customer | s: uuid_col | ") [:hints :table])))))

(deftest test-join-from-a-name-access-policy
  (testing "A masked column can be joined on: the policy masks what comes back"
    (let [policy {:type "column-type" :allow ["integer" "uuid"] :active true}]
      (is (re-find #"'xxxxx' AS \"data\""
                   (:query (eval/build-query (state "customer | unnest: data as company_id | company | s: c_0.data" [policy]))))))))
