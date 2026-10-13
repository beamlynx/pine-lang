(ns pine.limit-per-test
  "`limit: N per <columns>`: the first N rows of each group. See
  docs/limit-per.md."
  (:require [clojure.string :as s]
            [clojure.test :refer [deftest is testing]]
            [pine.api :as api]
            [pine.ast.main :as ast]
            [pine.eval :as eval]
            [pine.parser :as parser]
            [pine.sql.transpile :as transpile]))

(defn- p [e] (parser/parse-or-fail e))

(defn- state
  ([expression] (state expression :test))
  ([expression connection-id]
   (-> expression parser/parse :result (ast/generate connection-id expression nil {}))))

(defn- sql
  ([expression] (sql expression :test))
  ([expression connection-id]
   (-> expression parser/parse :result (ast/generate connection-id nil nil {} []) eval/build-query)))

(defn- sql-expressions
  "The SQL of the last expression, with the named results of the ones
  before it."
  [expressions]
  (let [{:keys [last-state]}
        (reduce (fn [{:keys [named-results]} expr]
                  (let [st (-> expr parser/parse :result (ast/generate :test nil nil named-results []))]
                    {:named-results (merge named-results (:pending-assignments st))
                     :last-state st}))
                {:named-results {} :last-state nil}
                expressions)]
    (eval/build-query last-state)))

(defn- hints [expression]
  (:hints (state expression)))

(defn- outer-select
  "The SELECT list of the outermost query: what the result's columns are."
  [query]
  (let [query (if (s/starts-with? query "WITH ")
                ;; After the last CTE.
                (subs query (+ 2 (s/last-index-of query ") SELECT ")))
                query)]
    (subs query 0 (s/index-of query " FROM "))))

(defn- occurrences [needle haystack]
  (count (re-seq (re-pattern (java.util.regex.Pattern/quote needle)) haystack)))

;; ---------------------------------------------------------------------------
;; Parser
;; ---------------------------------------------------------------------------

(deftest test-parse-limit-per
  (testing "`per` and the columns after it"
    (is (= {:type :limit :value 1 :per [{:column "customer_id"}]}
           (last (p "order | o: created_at desc | limit: 1 per customer_id"))))
    (is (= {:type :limit :value 1 :per [{:column "customer_id"}]}
           (last (p "order | o: created_at desc | l: 1 per customer_id"))))
    (is (= {:type :limit :value 3 :per [{:column "department_id"}]}
           (last (p "employee | o: salary desc | l: 3 per department_id")))))

  (testing "several columns"
    (is (= {:type :limit :value 1 :per [{:column "customer_id"} {:column "region"}]}
           (last (p "order | l: 1 per customer_id, region")))))

  (testing "an alias-qualified column"
    (is (= {:type :limit :value 1 :per [{:alias "c" :column "id"}]}
           (last (p "customer as c | order .customer_id | o: created_at desc | l: 1 per c.id")))))

  (testing "a key inside a JSON column"
    (is (= {:type :limit :value 1 :per [{:column "data" :path ["region"]}]}
           (last (p "customer | l: 1 per data.'region'")))))

  (testing "steps after it"
    (is (= [{:type :table :value {:table "ticket_status"}}
            {:type :order :value [{:column "created_at" :direction "DESC"}]}
            {:type :limit :value 1 :per [{:column "ticket_id"}]}
            {:type :where :value [{:type :column :value [nil "status" nil]} "=" {:type :string :value "open"}]}]
           (p "ticket_status | o: created_at desc | l: 1 per ticket_id | where: status = 'open'"))))

  (testing "a plain limit is unchanged: no :per"
    (is (= {:type :limit :value 10} (last (p "order | l: 10"))))
    (is (= {:type :limit :value 10} (last (p "order | 10")))))

  (testing "`per` needs whitespace on both sides"
    (is (= {:type :limit :value 1 :per [{:column "perimeter"}]} (last (p "shape | l: 1 per perimeter"))))
    (is (= {:type :limit :value 1 :per [{:column "per"}]} (last (p "shape | l: 1 per per"))))
    (is (:error (parser/parse "shape | l: 1perimeter")))
    (is (:error (parser/parse "shape | l: 1 permit")))
    (is (:error (parser/parse "shape | l: 1 per, a"))))

  (testing "a column called `perimeter` elsewhere is still a column"
    (is (= [{:type :table :value {:table "shape"}}
            {:type :select :value [{:column "perimeter"}]}
            {:type :order :value [{:column "perimeter" :direction "DESC"}]}
            {:type :where :value [{:type :column :value [nil "perimeter" nil]} ">" {:type :number :value 1}]}
            {:type :group :value {:columns [{:column "perimeter"}] :functions [{:kind :aggregate :fn "count" :arg nil :filter nil}]}}]
           (p "shape | s: perimeter | o: perimeter | w: perimeter > 1 | g: perimeter"))))

  (testing "a date bucket can't follow `per`"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"can't be used after `per`"
                          (p "order | l: 1 per created_at => month"))))

  (testing "the number is checked as for a plain limit"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"limit: must be between 0 and 10000"
                          (p "order | l: 10001 per customer_id")))))

(deftest test-parse-limit-per-partial
  (testing "while the columns after `per` are typed"
    (is (= {:type :limit-partial :value 1 :per []} (last (p "order | l: 1 per"))))
    (is (= {:type :limit-partial :value 1 :per []} (last (p "order | l: 1 per "))))
    (is (= {:type :limit-partial :value 1 :per [{:column "a"}]} (last (p "order | l: 1 per a,"))))
    (is (= {:type :limit-partial :value 1 :per [{:column "a"}]} (last (p "order | l: 1 per a, "))))
    (is (= {:type :limit-partial :value 1 :per [] :partial-alias {:alias "c" :column ""}}
           (last (p "order | l: 1 per c."))))
    (is (= {:type :limit-partial :value 1 :per [{:column "a"}] :partial-alias {:alias "c" :column ""}}
           (last (p "order | l: 1 per a, c."))))))

;; ---------------------------------------------------------------------------
;; AST
;; ---------------------------------------------------------------------------

(deftest test-limit-per-ast
  (testing "the per columns are resolved like order: columns"
    (let [{:keys [limit limit-per]} (state "employee | o: id desc | l: 3 per company_id")]
      (is (nil? limit))
      (is (= 3 (:n limit-per)))
      (is (= [{:alias "e_0" :column "company_id" :operation-index 2}] (:columns limit-per)))
      (is (= [{:alias "e_0" :column "id" :direction "DESC" :operation-index 1}] (:order limit-per)))))

  (testing "an alias-qualified column after a join"
    (is (= [{:alias "c" :column "id" :operation-index 3}]
           (-> "customer as c | order .customer_id | o: id desc | l: 1 per c.id" state :limit-per :columns))))

  (testing "a path into a JSON column"
    (is (= [{:alias "c_0" :column "data" :path ["region"] :column-alias "data.region" :operation-index 1}]
           (-> "customer | l: 1 per data.region" state :limit-per :columns))))

  (testing "with no order:, the rows are ranked by primary key"
    (is (= [{:alias "e_0" :column "id" :direction "ASC"}]
           (-> "employee | l: 1 per company_id" state :limit-per :order)))
    ;; The key, not whatever is called `id`
    (is (= [{:alias "l_0" :column "code" :direction "ASC"}]
           (-> "p.legacy | l: 1 per name" state :limit-per :order)))
    ;; A composite key, in key order
    (is (= [{:alias "m_0" :column "group_code" :direction "ASC"}
            {:alias "m_0" :column "member_code" :direction "ASC"}]
           (-> "p.membership | l: 1 per role" state :limit-per :order)))
    ;; No key: no order
    (is (= [] (-> "p.log_entry | l: 1 per message" state :limit-per :order)))
    ;; After a join, the current table's key first, then the others'
    (is (= [{:alias "o_1" :column "id" :direction "ASC"} {:alias "c" :column "id" :direction "ASC"}]
           (-> "customer as c | order .customer_id | l: 1 per c.id" state :limit-per :order)))))

;; ---------------------------------------------------------------------------
;; Hints
;; ---------------------------------------------------------------------------

(deftest test-limit-per-hints
  (testing "after `per`, every column of the current table"
    (is (= ["id" "company_id" "reports_to"] (->> "y.employee | l: 1 per " hints :select (map :column))))
    (is (= ["id" "company_id" "reports_to"] (->> "y.employee | l: 1 per" hints :select (map :column)))))

  (testing "after `per a, `, the columns not used yet"
    (is (= ["id" "reports_to"] (->> "y.employee | l: 1 per company_id, " hints :select (map :column)))))

  (testing "after `per c.`, the columns of that alias"
    (is (= ["id" "company_id" "reports_to"]
           (->> "x.company as c | y.employee as e | l: 1 per e." hints :select (map :column)))))

  (testing "while a column is typed, the columns that match it"
    (is (= ["company_id"] (->> "y.employee | l: 1 per comp" hints :select (map :column)))))

  (testing "a plain limit has no column hints"
    (is (= [] (-> "y.employee | l: 10" hints :select)))))

;; ---------------------------------------------------------------------------
;; SQL
;; ---------------------------------------------------------------------------

(deftest test-limit-per-sql
  (testing "top N per group: a row number per group, kept up to N, in the order of order:"
    (is (= {:query (str "SELECT \"__pine_rows__\".\"__e_0__id\", \"__pine_rows__\".\"id\", \"__pine_rows__\".\"company_id\", \"__pine_rows__\".\"reports_to\" "
                        "FROM ( SELECT \"e_0\".\"id\" AS \"__e_0__id\", \"e_0\".\"id\", \"e_0\".\"company_id\", \"e_0\".\"reports_to\", "
                        "ROW_NUMBER() OVER (PARTITION BY \"e_0\".\"company_id\" ORDER BY \"e_0\".\"id\" DESC) AS \"__pine_row__\", "
                        "ROW_NUMBER() OVER (ORDER BY \"e_0\".\"id\" DESC) AS \"__pine_order__\" "
                        "FROM \"employee\" AS \"e_0\" ) AS \"__pine_rows__\" "
                        "WHERE \"__pine_rows__\".\"__pine_row__\" <= 3 "
                        "ORDER BY \"__pine_rows__\".\"__pine_order__\" LIMIT 250")
            :params nil}
           (sql "employee | o: id desc | l: 3 per company_id"))))

  (testing "the order: column doesn't have to be in the result"
    (is (s/includes? (:query (sql "order | o: created_at desc | l: 1 per customer_id"))
                     "ROW_NUMBER() OVER (PARTITION BY \"o_0\".\"customer_id\" ORDER BY \"o_0\".\"created_at\" DESC) AS \"__pine_row__\"")))

  (testing "several partition columns"
    (is (s/includes? (:query (sql "order | o: id desc | l: 1 per customer_id, user_id"))
                     "PARTITION BY \"o_0\".\"customer_id\", \"o_0\".\"user_id\" ORDER BY")))

  (testing "an alias-qualified column after a join"
    (let [{:keys [query]} (sql "customer as c | order .customer_id | o: id desc | l: 1 per c.id")]
      (is (s/includes? query "PARTITION BY \"c\".\"id\" ORDER BY \"o_1\".\"id\" DESC"))
      (is (s/includes? query "JOIN \"order\" AS \"o_1\""))))

  (testing "the row numbers never reach the result"
    (doseq [e ["employee | o: id desc | l: 3 per company_id"
               "employee | l: 3 per company_id"
               "employee | o: id desc | l: 1 per company_id | where: reports_to = 1"
               "employee | o: id desc | l: 1 per company_id | document"
               "employee | o: id desc | l: 1 per company_id |= latest | document"]]
      (let [select (outer-select (:query (sql e)))]
        (is (not (s/includes? select "__pine_row__")) e)
        (is (not (s/includes? select "__pine_order__")) e))))

  (testing "an explicit select: is kept as written"
    (is (s/starts-with? (:query (sql "employee | s: id, reports_to as boss | o: id desc | l: 1 per company_id"))
                        (str "SELECT \"__pine_rows__\".\"id\", \"__pine_rows__\".\"boss\", \"__pine_rows__\".\"__e_0__id\" "
                             "FROM ( SELECT \"e_0\".\"id\", \"e_0\".\"reports_to\" AS \"boss\", \"e_0\".\"id\" AS \"__e_0__id\", "))))

  (testing "two columns with the same name: each named after its position inside, by its own name outside"
    (let [{:keys [query]} (sql "customer as c | order .customer_id | s: c.id | l: 1 per c.id")]
      (is (s/starts-with? query (str "SELECT \"__pine_rows__\".\"__pine_c0__\" AS \"id\", \"__pine_rows__\".\"__c__id\", "
                                     "\"__pine_rows__\".\"__o_1__id\", \"__pine_rows__\".\"__pine_c3__\" AS \"id\", ")))
      (is (s/includes? query "SELECT \"c\".\"id\" AS \"__pine_c0__\", \"c\".\"id\" AS \"__c__id\", \"o_1\".\"id\" AS \"__o_1__id\", \"o_1\".\"id\" AS \"__pine_c3__\", "))))

  (testing "a key inside a JSON column groups by its value as text"
    (is (= {:query (str "SELECT \"__pine_rows__\".\"__c_0__id\", \"__pine_rows__\".\"id\", \"__pine_rows__\".\"data\", \"__pine_rows__\".\"uuid_col\" "
                        "FROM ( SELECT \"c_0\".\"id\" AS \"__c_0__id\", \"c_0\".\"id\", \"c_0\".\"data\", \"c_0\".\"uuid_col\", "
                        "ROW_NUMBER() OVER (PARTITION BY jsonb_extract_path_text(\"c_0\".\"data\"::jsonb, ?::text) ORDER BY \"c_0\".\"id\" ASC) AS \"__pine_row__\" "
                        "FROM \"customer\" AS \"c_0\" ) AS \"__pine_rows__\" WHERE \"__pine_rows__\".\"__pine_row__\" <= 1 LIMIT 250")
            :params [{:type :string :value "region"}]}
           (update (sql "customer | l: 1 per data.region") :params vec))))

  (testing "where: before it filters the rows before they are ranked"
    (is (s/includes? (:query (sql "employee | where: reports_to = 1 | o: id desc | l: 1 per company_id"))
                     "FROM \"employee\" AS \"e_0\" WHERE \"e_0\".\"reports_to\" = ? ) AS \"__pine_rows__\""))))

(deftest test-limit-per-key-fallback
  (testing "no order: ranks each group by primary key"
    (is (s/includes? (:query (sql "employee | l: 1 per company_id"))
                     "ROW_NUMBER() OVER (PARTITION BY \"e_0\".\"company_id\" ORDER BY \"e_0\".\"id\" ASC) AS \"__pine_row__\""))
    (is (s/includes? (:query (sql "p.legacy | l: 1 per name"))
                     "OVER (PARTITION BY \"l_0\".\"name\" ORDER BY \"l_0\".\"code\" ASC)"))
    (is (s/includes? (:query (sql "p.membership | l: 1 per role"))
                     "OVER (PARTITION BY \"m_0\".\"role\" ORDER BY \"m_0\".\"group_code\" ASC, \"m_0\".\"member_code\" ASC)")))

  (testing "no order: and no primary key: the database picks"
    (is (s/includes? (:query (sql "p.log_entry | l: 1 per message"))
                     "ROW_NUMBER() OVER (PARTITION BY \"le_0\".\"message\") AS \"__pine_row__\"")))

  (testing "no order: on a named result: it has no primary key"
    (is (s/includes? (:query (sql-expressions ["employee |= staff" "staff | l: 1 per company_id"]))
                     "ROW_NUMBER() OVER (PARTITION BY \"staff\".\"company_id\") AS \"__pine_row__\"")))

  (testing "without order: the result has no overall order"
    (let [{:keys [query]} (sql "employee | l: 1 per company_id")]
      (is (not (s/includes? query "__pine_order__")))
      (is (s/ends-with? query "WHERE \"__pine_rows__\".\"__pine_row__\" <= 1 LIMIT 250")))))

(def ^:private sealed-body
  (str "SELECT \"__pine_rows__\".\"id\", \"__pine_rows__\".\"company_id\", \"__pine_rows__\".\"reports_to\" "
       "FROM ( SELECT \"e_0\".\"id\", \"e_0\".\"company_id\", \"e_0\".\"reports_to\", "
       "ROW_NUMBER() OVER (PARTITION BY \"e_0\".\"company_id\" ORDER BY \"e_0\".\"id\" DESC) AS \"__pine_row__\", "
       "ROW_NUMBER() OVER (ORDER BY \"e_0\".\"id\" DESC) AS \"__pine_order__\" "
       "FROM \"employee\" AS \"e_0\" ) AS \"__pine_rows__\" "
       "WHERE \"__pine_rows__\".\"__pine_row__\" <= 1 ORDER BY \"__pine_rows__\".\"__pine_order__\""))

(deftest test-limit-per-checkpoint
  (testing "where: after it filters the rows kept per group"
    (is (= {:query (str "WITH \"result_1\" AS ( " sealed-body " ) "
                        "SELECT \"result_1\".* FROM \"result_1\" AS \"result_1\" WHERE \"result_1\".\"reports_to\" = ? LIMIT 250")
            :params [{:type :number :value 1}]}
           (update (sql "employee | o: id desc | l: 1 per company_id | where: reports_to = 1") :params vec))))

  (testing "a join after it starts from the rows kept per group"
    (is (= {:query (str "WITH \"result_1\" AS ( " sealed-body " ) "
                        "SELECT \"d_1\".\"id\" AS \"__d_1__id\", \"d_1\".* FROM \"result_1\" AS \"result_1\" "
                        "JOIN \"document\" AS \"d_1\" ON \"result_1\".\"id\" = \"d_1\".\"employee_id\" LIMIT 250")
            :params nil}
           (sql "employee | o: id desc | l: 1 per company_id | document"))))

  (testing "`|= name` after it names the rows kept per group"
    (is (= {:query (str "WITH \"latest\" AS ( " sealed-body " ) "
                        "SELECT \"d_1\".\"id\" AS \"__d_1__id\", \"d_1\".* FROM \"latest\" AS \"latest\" "
                        "JOIN \"document\" AS \"d_1\" ON \"latest\".\"id\" = \"d_1\".\"employee_id\" LIMIT 250")
            :params nil}
           (sql "employee | o: id desc | l: 1 per company_id |= latest | document")))
    (testing "and in a later expression"
      (is (= {:query (str "WITH \"latest\" AS ( " sealed-body " ) "
                          "SELECT \"latest\".* FROM \"latest\" AS \"latest\" WHERE \"latest\".\"reports_to\" = ? LIMIT 250")
              :params [{:type :number :value 1}]}
             (update (sql-expressions ["employee | o: id desc | l: 1 per company_id |= latest"
                                       "latest | where: reports_to = 1"])
                     :params vec)))))

  (testing "the per-group cut is made once, inside the CTE"
    (doseq [e ["employee | o: id desc | l: 1 per company_id | where: reports_to = 1"
               "employee | o: id desc | l: 1 per company_id | document"
               "employee | o: id desc | l: 1 per company_id |= latest | document"]]
      (is (= 1 (occurrences "ROW_NUMBER() OVER (PARTITION BY" (:query (sql e)))) e)))

  (testing "a named result's columns leave the row numbers out"
    (is (= ["id" "company_id" "reports_to"]
           (->> (-> (ast/generate (:result (parser/parse "latest | s: "))
                                  :test "latest | s: " nil
                                  (:pending-assignments (state "employee | o: id desc | l: 1 per company_id |= latest")))
                    :hints :select)
                (map :column)))))

  (testing "count: counts the rows kept"
    (is (= {:query (str "WITH x AS ( " sealed-body " ) SELECT COUNT(*) FROM x") :params nil}
           (sql "employee | o: id desc | l: 1 per company_id | count:")))))

(deftest test-limit-per-writes
  (testing "delete! removes the rows kept per group"
    (is (= {:query (str "DELETE FROM \"employee\" WHERE \"id\" IN ( SELECT \"__pine_rows__\".\"id\" "
                        "FROM ( SELECT \"e_0\".\"id\", ROW_NUMBER() OVER (PARTITION BY \"e_0\".\"company_id\" ORDER BY \"e_0\".\"id\" DESC) AS \"__pine_row__\", "
                        "ROW_NUMBER() OVER (ORDER BY \"e_0\".\"id\" DESC) AS \"__pine_order__\" FROM \"employee\" AS \"e_0\" ) AS \"__pine_rows__\" "
                        "WHERE \"__pine_rows__\".\"__pine_row__\" <= 1 ORDER BY \"__pine_rows__\".\"__pine_order__\" )")
            :params nil}
           (sql "employee | o: id desc | l: 1 per company_id | delete! .id")))))

(deftest test-limit-per-dialects
  (testing "MySQL quotes with backticks"
    (is (= {:query (str "SELECT `__pine_rows__`.`__e_0__id`, `__pine_rows__`.`id`, `__pine_rows__`.`company_id`, `__pine_rows__`.`reports_to` "
                        "FROM ( SELECT `e_0`.`id` AS `__e_0__id`, `e_0`.`id`, `e_0`.`company_id`, `e_0`.`reports_to`, "
                        "ROW_NUMBER() OVER (PARTITION BY `e_0`.`company_id` ORDER BY `e_0`.`id` DESC) AS `__pine_row__`, "
                        "ROW_NUMBER() OVER (ORDER BY `e_0`.`id` DESC) AS `__pine_order__` "
                        "FROM `employee` AS `e_0` ) AS `__pine_rows__` "
                        "WHERE `__pine_rows__`.`__pine_row__` <= 3 "
                        "ORDER BY `__pine_rows__`.`__pine_order__` LIMIT 250")
            :params nil}
           (sql "employee | o: id desc | l: 3 per company_id" :test-mysql))))

  (testing "MySQL refuses two columns of one name in a derived table, so they are renamed inside"
    (let [{:keys [query]} (sql "customer as c | order .customer_id | s: c.id | l: 1 per c.id" :test-mysql)]
      (is (s/includes? query "SELECT `c`.`id` AS `__pine_c0__`, `c`.`id` AS `__c__id`, `o_1`.`id` AS `__o_1__id`, `o_1`.`id` AS `__pine_c3__`, "))
      (is (s/starts-with? query "SELECT `__pine_rows__`.`__pine_c0__` AS `id`, "))))

  (testing "MySQL: a key inside a JSON column"
    (is (s/includes? (:query (sql "customer | l: 1 per data.region" :test-mysql))
                     "PARTITION BY JSON_UNQUOTE(JSON_EXTRACT(`c_0`.`data`, ?))")))

  (testing "MySQL: delete! wraps the subquery"
    (is (s/starts-with? (:query (sql "employee | o: id desc | l: 1 per company_id | delete! .id" :test-mysql))
                        "DELETE FROM `employee` WHERE `id` IN ( SELECT * FROM ( SELECT `__pine_rows__`.`id` FROM ( ")))

  (testing "SQLite quotes like Postgres"
    (is (= (:query (sql "employee | o: id desc | l: 3 per company_id"))
           (:query (sql "employee | o: id desc | l: 3 per company_id" :test-sqlite)))))

  (testing "SQLite: a key inside a JSON column"
    (is (s/includes? (:query (sql "customer | l: 1 per data.region" :test-sqlite))
                     "PARTITION BY json_extract(\"c_0\".\"data\", ?)"))))

(deftest test-limit-per-access-policy
  (let [rules [{:type "column-type" :allow ["integer"]}]
        sql (fn [e] (-> e parser/parse :result (ast/generate :test nil nil {} rules) eval/build-query))]
    (testing "a column the policy hides can't follow `per`: the groups would give its values away"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"`r_0.title` is hidden by the access policy, so it can't be used in where:, order:, group: or after `per`"
                            (sql "report | l: 1 per title"))))
    (testing "the columns of `*` are redacted inside, and named outside"
      (let [{:keys [query]} (sql "report | l: 1 per id")]
        (is (s/includes? query "'xxxxx' AS \"title\""))
        (is (s/starts-with? query "SELECT \"__pine_rows__\".\"__r_0__id\", \"__pine_rows__\".\"id\", \"__pine_rows__\".\"title\" FROM ( "))))))

(deftest test-limit-per-params
  (testing "parameters are in the order their `?` appear: per column, order:, overall order, then where:"
    (is (= [{:type :string :value "region"} {:type :string :value "age"} {:type :string :value "age"} {:type :number :value 1}]
           (vec (:params (sql "customer | where: id = 1 | o: data.age | l: 1 per data.region")))))))

(deftest test-limit-per-api
  (let [result (api/api-build "employee | o: id desc | l: 1 per company_id" nil :test)]
    (testing "the SQL without hidden columns leaves out the hidden key, inside and outside"
      (is (not (s/includes? (:query-without-hidden result) "__e_0__id")))
      (is (s/starts-with? (s/triml (:query-without-hidden result))
                          "SELECT \"__pine_rows__\".\"id\", \"__pine_rows__\".\"company_id\", \"__pine_rows__\".\"reports_to\" FROM ( SELECT \"e_0\".\"id\", ")))
    (testing "SQL to Pine refuses the SQL of a per query cleanly: it has a subquery"
      (is (= "sql-unsupported"
             (:error-type (transpile/sql->pine (s/trim (s/replace (:query-without-hidden result) #";\s*$" "")))))))))
