(ns pine.named-results-in-test
  "A named result used after `in`: the values its one column returns."
  (:require [clojure.test :refer [deftest is testing]]
            [pine.api :as api]
            [pine.ast.main :as ast]
            [pine.eval :as eval]
            [pine.parser :as parser]))

(defn- generate
  "SQL for the last of several blocks, threading named results like api.clj."
  [expressions]
  (let [{:keys [state]}
        (reduce (fn [{:keys [variables]} expression]
                  (let [st (ast/generate (:result (parser/parse expression)) :test nil nil variables [])]
                    {:variables (merge variables (:pending-assignments st)) :state st}))
                {:variables {}}
                expressions)]
    (eval/build-query state)))

(def ^:private acme-employees "company | where: name = 'Acme' | employee .company_id | s: id |= acme_emps")

(deftest test-in-named-result
  (testing "in <named result> is IN ( SELECT its column FROM it ), with its CTE and parameters first"
    (is (= {:query (str "WITH \"acme_emps\" AS ( SELECT \"e_1\".\"id\" FROM \"company\" AS \"c_0\" JOIN \"employee\" AS \"e_1\" "
                        "ON \"c_0\".\"id\" = \"e_1\".\"company_id\" WHERE \"c_0\".\"name\" = ? ) "
                        "SELECT \"e_0\".id AS \"__e_0__id\", \"e_0\".* FROM \"employee\" AS \"e_0\" "
                        "WHERE \"e_0\".\"id\" IN ( SELECT \"id\" FROM \"acme_emps\" ) LIMIT 250")
            :params [{:type :string :value "Acme"}]}
           (update (generate [acme-employees "employee | where: id in acme_emps"]) :params vec))))

  (testing "not in"
    (is (re-find #"WHERE \"e_0\".\"id\" NOT IN \( SELECT \"id\" FROM \"acme_emps\" \)"
                 (:query (generate [acme-employees "employee | where: id not in acme_emps"])))))

  (testing "a column alias is the name it's selected by"
    (is (re-find #"IN \( SELECT \"emp\" FROM \"x\" \)"
                 (:query (generate ["employee | s: id as emp |= x" "employee | where: id in x"]))))))

(deftest test-where-it-can-appear
  (testing "inside an or group, with the condition's other parameters after the CTE's"
    (let [{:keys [query params]} (generate [acme-employees "employee | where: id in acme_emps or name = 'Bob'"])]
      (is (re-find #"WHERE \(\"e_0\".\"id\" IN \( SELECT \"id\" FROM \"acme_emps\" \) OR \"e_0\".\"name\" = \?\)" query))
      (is (= ["Acme" "Bob"] (map :value params)))))

  (testing "before a group:, which builds its own WITH list"
    (let [{:keys [query]} (generate [acme-employees "employee | where: id in acme_emps | g: name => count"])]
      (is (re-find #"^WITH \"acme_emps\" AS \(" query))
      (is (re-find #"IN \( SELECT \"id\" FROM \"acme_emps\" \)" query))))

  (testing "defined earlier in the same expression"
    (is (re-find #"IN \( SELECT \"id\" FROM \"x\" \)"
                 (:query (generate ["employee | s: id |= x | company | where: id in x"])))))

  (testing "inside another named result: both CTEs, dependency first"
    (let [{:keys [query]} (generate [acme-employees
                                     "employee | where: id in acme_emps | s: id |= again"
                                     "employee | where: id in again"])]
      (is (re-find #"^WITH \"acme_emps\" AS \(.*\), \"again\" AS \(.*IN \( SELECT \"id\" FROM \"acme_emps\" \).*\) SELECT" query)))))

(deftest test-errors
  (testing "not a named result"
    (is (thrown-with-msg? Exception #"`nope` after `in` must be a named result"
                          (generate ["employee | where: id in nope"]))))
  (testing "every column"
    (is (thrown-with-msg? Exception #"selects every column.*\| s: id \|= c"
                          (generate ["company |= c" "employee | where: company_id in c"]))))
  (testing "more than one column, named"
    (is (thrown-with-msg? Exception #"selects 2 columns \(id, name\)"
                          (generate ["company | s: id, name |= c" "employee | where: company_id in c"])))))

(deftest test-through-the-api
  (testing "with a values block feeding the named result"
    (let [response (:body (api/app-routes {:request-method :post
                                           :uri "/api/v1/build"
                                           :params {:expressions ["$company = 'Acme'"
                                                                  "company | where: name = $company | employee .company_id | s: id |= acme_emps"
                                                                  "employee | where: id in acme_emps"]
                                                    :connection-id :test}}))]
      (is (nil? (:error response)))
      (is (re-find #"WITH \"acme_emps\" AS \(.*'Acme'.*\).*IN \( SELECT \"id\" FROM \"acme_emps\" \)" (:query response))))))
