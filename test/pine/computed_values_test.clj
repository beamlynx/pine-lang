(ns pine.computed-values-test
  "Computed values: computed columns in select:, function calls, conditions
  as values, if/then/else, aggregates over a value in group:, now(), and
  casts in a join pair. See docs/computed-values.md."
  (:require [clojure.java.io :as io]
            [clojure.string :as s]
            [clojure.test :refer [deftest is testing]]
            [instaparse.core :as insta]
            [pine.api :as api]
            [pine.ast.main :as ast]
            [pine.eval :as eval]
            [pine.parser :as parser]
            [pine.variables :as variables]))

(defn- build
  "{:query :params} for one expression, or for several where the last is
  built with the named results of the ones before."
  ([expression] (build :test expression []))
  ([connection expression] (build connection expression []))
  ([connection expression policy]
   (let [expressions (if (string? expression) [expression] expression)
         {:keys [state]} (reduce (fn [{:keys [named-results]} e]
                                   (let [state (-> e parser/parse :result variables/bind
                                                   (ast/generate connection nil nil named-results policy))]
                                     {:named-results (merge named-results (:pending-assignments state))
                                      :state state}))
                                 {:named-results {}}
                                 expressions)]
     (eval/build-query state))))

(defn- sql
  "The query with its parameters filled in, as the SQL preview shows it."
  [& args]
  (s/trim (eval/formatted-query (apply build args))))

(defn- error [& args]
  (try (apply sql args) nil
       (catch clojure.lang.ExceptionInfo e (.getMessage e))))

(defn- parse-error [expression]
  (or (:error (try (parser/parse expression) (catch clojure.lang.ExceptionInfo e {:error (.getMessage e)})))
      (str "parsed: " expression)))

;; -------
;; GRAMMAR
;; -------

(def ^:private grammar (insta/parser (slurp (io/resource "pine/pine.bnf"))))

(deftest test-one-reading
  (testing "Each computed value reads one way"
    (doseq [text ["coalesce(reference_id, concat('att:', a.id)) as dedupe_key"
                  "id in edited as was_edited"
                  "if x = 'a' then 'b' else if y = 'c' then d else e as k"
                  "if x = 'a' then (if y = 'b' then 1 else 2) else 3 as k"
                  "a.id::text as k"
                  "a.id ::text as k"
                  "now() as t"
                  "(e.document_id = 1) as k"
                  "x in (1, 2) or y like 'a%' as k"
                  "concat($prefix, name) as k"]]
      (is (= 1 (count (insta/parses grammar text :start :computed-column))) text)))

  (testing "Each aggregate reads one way"
    (doseq [text ["count"
                  "count where a.type = 'x'"
                  "min(a.id::text) where a.type = 'sharepoint' as sp_att"
                  "any(a.type = 'sharepoint') as on_sp"
                  "all(a.id > 1)"]]
      (is (= 1 (count (insta/parses grammar text :start :aggregate-function))) text))))

(deftest test-parse
  (testing "A computed column is its value and its name"
    (is (= [{:type :table :value {:table "t"}}
            {:type :select
             :value [{:column-alias "k"
                      :computed {:kind :call :fn "coalesce"
                                 :args [{:kind :column :alias nil :column "a"}
                                        {:kind :literal :value {:type :string :value "x"}}]}}]}]
           (:result (parser/parse "t | s: coalesce(a, 'x') as k")))))

  (testing "`s: a as b` is still a plain select"
    (is (= :select (:type (second (:result (parser/parse "t | s: a as b"))))))
    (is (= :select-partial (:type (second (:result (parser/parse "t | s: a as b,")))))))

  (testing "A keyword literal is a value, not a column"
    (is (= {:kind :literal :value {:type :symbol :value "true"}}
           (-> (parser/parse "t | s: true as t") :result second :value first :computed))))

  (testing "else if is a chain of ifs"
    (is (= :if (-> (parser/parse "t | s: if a = 1 then 'x' else if a = 2 then 'y' as k") :result second :value first :computed :else :kind))))

  (testing "A cast on either side of a join pair"
    (is (= [{:left "id" :right "resourceableId" :left-cast "text"}]
           (-> (parser/parse "g | activity_resource .resourceableId = .id ::text") :result second :value :join-column-pairs)))
    (is (= [{:left "id" :right "x" :right-cast "uuid"}]
           (-> (parser/parse "g | t .x::uuid = .id") :result second :value :join-column-pairs))))

  (testing "Refused"
    (is (s/includes? (parse-error "t | s: lower(name) as n") "`lower()` isn't a Pine function. There are coalesce(), concat(), now(), and in group: all, any, avg, count, max, min, sum."))
    (is (s/includes? (parse-error "t | s: coalesce(a) as n") "`coalesce()` takes 2 or more arguments, not 1."))
    (is (s/includes? (parse-error "t | s: now(a) as n") "`now()` takes 0 arguments, not 1."))
    (is (s/includes? (parse-error "t | s: min(a) as n") "`min()` is an aggregate. Use it after `=>` in group:"))
    (is (s/includes? (parse-error "t | group: a => coalesce(b, c)") "`coalesce()` isn't an aggregate."))
    (is (s/includes? (parse-error "t | group: a => min(b)::text") "A cast can't follow an aggregate."))
    (is (s/includes? (parse-error "t | s: coalesce(a, b)") "A computed value in select: needs a name"))))

;; ------
;; VALUES
;; ------

(deftest test-functions
  (testing "coalesce and concat"
    (is (= "SELECT COALESCE(\"r_0\".\"title\", (('r:')::text || (\"r_0\".\"id\")::text)) AS \"k\", \"r_0\".\"id\" AS \"__r_0__id\" FROM \"report\" AS \"r_0\" LIMIT 250;"
           (sql "report | s: coalesce(title, concat('r:', id)) as k")))
    (is (= "SELECT COALESCE(`r_0`.`title`, CONCAT('r:', `r_0`.`id`)) AS `k`, `r_0`.`id` AS `__r_0__id` FROM `report` AS `r_0` LIMIT 250;"
           (sql :test-mysql "report | s: coalesce(title, concat('r:', id)) as k")))
    (is (= "SELECT COALESCE(\"r_0\".\"title\", ('r:' || \"r_0\".\"id\")) AS \"k\", \"r_0\".\"id\" AS \"__r_0__id\" FROM \"report\" AS \"r_0\" LIMIT 250;"
           (sql :test-sqlite "report | s: coalesce(title, concat('r:', id)) as k"))))

  (testing "A computed column replaces `t.*`, as a plain column does"
    (is (s/starts-with? (sql "report | s: now() as t") "SELECT NOW() AS \"t\", \"r_0\".\"id\" AS \"__r_0__id\" FROM")))

  (testing "now() on each database"
    (is (s/includes? (sql "company | where: created_at > now()") "WHERE \"c_0\".\"created_at\" > NOW()"))
    (is (s/includes? (sql :test-mysql "company | where: created_at > now()") "WHERE `c_0`.`created_at` > NOW()"))
    (is (s/includes? (sql :test-sqlite "company | where: created_at > now()") "WHERE \"c_0\".\"created_at\" > CURRENT_TIMESTAMP")))

  (testing "Casts inside a value"
    (is (s/includes? (sql "report | s: coalesce(id::text, title) as k") "COALESCE(\"r_0\".\"id\"::text, \"r_0\".\"title\")"))
    (is (s/includes? (sql :test-mysql "report | s: coalesce(id::text, title) as k") "COALESCE(CAST(`r_0`.`id` AS CHAR), `r_0`.`title`)"))
    (is (s/includes? (sql :test-sqlite "report | s: coalesce(id ::text, title) as k") "COALESCE(CAST(\"r_0\".\"id\" AS TEXT), \"r_0\".\"title\")")))

  (testing "A literal beside a column takes its type"
    (let [{:keys [query params]} (build "customer | s: coalesce(uuid_col, '00000000-0000-0000-0000-000000000000') as k")]
      (is (s/includes? query "COALESCE(\"c_0\".\"uuid_col\", ?::uuid)"))
      (is (= :uuid (:type (first params))))))

  (testing "A key inside a JSON column, as text"
    (is (s/includes? (sql "customer | s: coalesce(data.plan, 'free') as plan")
                     "COALESCE(jsonb_extract_path_text(\"c_0\".\"data\"::jsonb, 'plan'::text), 'free') AS \"plan\"")))

  (testing "A $variable"
    (let [state (fn [bindings] (-> "report | s: coalesce(title, $fallback) as k" parser/parse :result
                                   (variables/bind bindings) (ast/generate :test nil nil {} []) eval/build-query))]
      (is (= [{:type :string :value "none"}] (:params (state {"fallback" "none"}))))
      (is (= [{:type :variable :value "fallback"}] (:params (state {})))))))

(deftest test-conditions-as-values
  (testing "A condition is a true/false column"
    (is (s/starts-with? (sql "report | s: title like 'A%' as a, id in (1, 2) or title is null as b")
                        "SELECT (\"r_0\".\"title\" LIKE 'A%') AS \"a\", (\"r_0\".\"id\" IN ('1', '2') OR \"r_0\".\"title\" IS NULL) AS \"b\"")))

  (testing "An enum compared inside one is cast, as in where:"
    (is (s/includes? (:query (build "attachment | s: type = 'sharepoint' as sp")) "(\"a_0\".\"type\" = ?::\"public\".\"attachment_type\") AS \"sp\"")))

  (testing "A named result after `in` emits its CTE"
    (is (= (str "WITH \"edited\" AS ( SELECT \"e_0\".\"company_id\" FROM \"employee\" AS \"e_0\" ) "
                "SELECT (\"c_0\".\"id\" IN ( SELECT \"company_id\" FROM \"edited\" )) AS \"was_edited\", \"c_0\".\"id\" AS \"__c_0__id\" "
                "FROM \"company\" AS \"c_0\" LIMIT 250;")
           (sql :test ["employee | s: company_id |= edited" "company | s: id in edited as was_edited"])))))

(deftest test-if
  (testing "Branches are one flat CASE"
    (is (s/includes? (sql "report | s: if id = 1 then 'one' else if id = 2 then 'two' else title as k")
                     "CASE WHEN (\"r_0\".\"id\" = '1') THEN 'one' WHEN (\"r_0\".\"id\" = '2') THEN 'two' ELSE \"r_0\".\"title\" END AS \"k\"")))

  (testing "No else is NULL"
    (is (s/includes? (sql "report | s: if title is null then id as k") "CASE WHEN (\"r_0\".\"title\" IS NULL) THEN \"r_0\".\"id\" END AS \"k\"")))

  (testing "An if inside then is in parentheses"
    (is (s/includes? (sql "report | s: if id = 1 then (if title is null then 'a' else 'b') else 'c' as k")
                     "CASE WHEN (\"r_0\".\"id\" = '1') THEN CASE WHEN (\"r_0\".\"title\" IS NULL) THEN 'a' ELSE 'b' END ELSE 'c' END"))))

;; ----------------
;; USING THE NAMES
;; ----------------

(deftest test-names
  (testing "where:, order:, per and group: inline the value"
    (let [{:keys [query params]} (build "report | s: coalesce(title, 'x') as k | where: k = 'y' | o: k desc")]
      (is (= (str "SELECT COALESCE(\"r_0\".\"title\", ?) AS \"k\", \"r_0\".\"id\" AS \"__r_0__id\" FROM \"report\" AS \"r_0\" "
                  "WHERE COALESCE(\"r_0\".\"title\", ?) = ? ORDER BY COALESCE(\"r_0\".\"title\", ?) DESC LIMIT 250")
             query))
      (is (= ["x" "x" "y" "x"] (map :value params)) "in the order their ? appear")))

  (testing "per on a computed name"
    (is (s/includes? (sql "report | s: id, coalesce(title, 'x') as k | o: id desc | l: 1 per k")
                     "ROW_NUMBER() OVER (PARTITION BY COALESCE(\"r_0\".\"title\", 'x') ORDER BY \"r_0\".\"id\" DESC) AS \"__pine_row__\"")))

  (testing "group: on a computed name, in a terminal group and in a checkpoint"
    (is (= "WITH \"x_2\" AS ( SELECT COALESCE(\"r_0\".\"title\", 'x') AS \"k\" FROM \"report\" AS \"r_0\" ) SELECT \"x_2\".\"k\", COUNT(1) AS \"count\" FROM \"x_2\" GROUP BY \"x_2\".\"k\" LIMIT 10000;"
           (sql "report | s: coalesce(title, 'x') as k | group: k")))
    (is (s/includes? (sql "report | s: coalesce(title, 'x') as k | group: k | where: count > 1")
                     "SELECT COALESCE(\"r_0\".\"title\", 'x') AS \"k\", COUNT(1) AS \"count\" FROM \"report\" AS \"r_0\" GROUP BY 1")))

  (testing "A condition's name compared with true"
    (is (s/includes? (sql "report | s: title is null as untitled | where: untitled = true") "WHERE (\"r_0\".\"title\" IS NULL) = true")))

  (testing "The name wins over a column with the same name; the alias still reaches the column"
    (is (s/includes? (sql "report as r | s: coalesce(title, 'x') as title | where: title = 'y' | where: r.title = 'z'")
                     "WHERE COALESCE(\"r\".\"title\", 'x') = 'y' AND \"r\".\"title\" = 'z'")))

  (testing "A computed value can use an earlier one's name"
    (is (s/includes? (sql "report | s: coalesce(title, 'x') as k, concat(k, '!') as k2")
                     "((COALESCE(\"r_0\".\"title\", 'x'))::text || ('!')::text) AS \"k2\"")))

  (testing "Two computed columns can't share a name"
    (is (= "`k` is already the name of a computed column. Give each one its own name."
           (error "report | s: now() as k, now() as k"))))

  (testing "After a checkpoint the name is the checkpoint's column"
    (is (s/ends-with? (sql "report | s: coalesce(title, 'x') as k | l: 10 | where: k = 'y'")
                      "WHERE \"result_1\".\"k\" = 'y' LIMIT 250;"))))

;; ----------
;; AGGREGATES
;; ----------

(deftest test-aggregates
  (testing "Over a value, with a filter, in a terminal group:"
    (is (= (str "WITH \"x_1\" AS ( SELECT \"e_0\".\"company_id\" AS \"company_id\", \"e_0\".\"id\" AS \"__pine_a0__\", \"e_0\".\"reports_to\" AS \"__pine_a1__\" FROM \"employee\" AS \"e_0\" ) "
                "SELECT \"x_1\".\"company_id\", COUNT(1) AS \"count\", MIN(\"x_1\".\"__pine_a0__\"::text) FILTER (WHERE (\"x_1\".\"__pine_a1__\" IS NULL)) AS \"top\", "
                "BOOL_OR((\"x_1\".\"__pine_a1__\" IS NULL)) AS \"any_reports_to\", MAX(\"x_1\".\"__pine_a0__\") AS \"max_id\", SUM(\"x_1\".\"__pine_a0__\") AS \"sum_id\", "
                "AVG(\"x_1\".\"__pine_a0__\") AS \"avg_id\", BOOL_AND((\"x_1\".\"__pine_a0__\" > '1')) AS \"all_id\" FROM \"x_1\" GROUP BY \"x_1\".\"company_id\" LIMIT 10000;")
           (sql "employee | group: company_id => count, min(id::text) where reports_to is null as top, any(reports_to is null), max(id), sum(id), avg(id), all(id > 1)"))))

  (testing "MySQL has no FILTER or BOOL_OR"
    (is (s/includes? (sql :test-mysql "employee | group: company_id => min(id) where reports_to is null as top, any(reports_to is null) as r, count where id > 1")
                     "MIN(CASE WHEN (`x_1`.`__pine_a1__` IS NULL) THEN `x_1`.`__pine_a0__` END) AS `top`, MAX((`x_1`.`__pine_a1__` IS NULL)) AS `r`, COUNT(CASE WHEN (`x_1`.`__pine_a0__` > '1') THEN 1 END) AS `count`")))

  (testing "SQLite has FILTER, and no BOOL_AND"
    (is (s/includes? (sql :test-sqlite "employee | group: company_id => min(id) where reports_to is null as top, all(id > 1) as a")
                     "MIN(\"x_1\".\"__pine_a0__\") FILTER (WHERE (\"x_1\".\"__pine_a1__\" IS NULL)) AS \"top\", MIN((\"x_1\".\"__pine_a0__\" > '1')) AS \"a\"")))

  (testing "A filter's string is a parameter after the inner query's"
    (let [{:keys [params]} (build "employee | where: id > 5 | group: company_id => count where reports_to = 3")]
      (is (= [5 3] (map :value params)))))

  (testing "In a checkpoint, then filtered by name: SQL's HAVING"
    (is (= (str "WITH \"result_1\" AS ( SELECT \"e_0\".\"company_id\", MIN(\"e_0\".\"id\") AS \"first\", BOOL_OR((\"e_0\".\"reports_to\" IS NULL)) AS \"top\" "
                "FROM \"employee\" AS \"e_0\" GROUP BY \"e_0\".\"company_id\" ) "
                "SELECT \"result_1\".* FROM \"result_1\" AS \"result_1\" WHERE (\"result_1\".\"top\" = true OR \"result_1\".\"first\" > '10') LIMIT 250;")
           (sql "employee | group: company_id => min(id) as first, any(reports_to is null) as top | where: top = true or first > 10"))))

  (testing "The checkpoint lists the aggregates' names"
    (is (= ["first" "top"] (:aggregates (first (get-in (api/api-build ["employee | group: company_id => min(id) as first, any(reports_to is null) as top"] nil :test) [:ast :checkpoints]))))))

  (testing "Two aggregates with the same name are refused"
    (is (= "Two aggregates are named `min_id`. Name them apart with `as`, like `min(a.id) as first_id`."
           (error "employee | group: company_id => min(id), min(id::text)")))))

;; -------
;; update!
;; -------

(deftest test-update
  (testing "now() as a value"
    (is (= "UPDATE \"company\" SET \"created_at\" = NOW() WHERE \"id\" IN ( SELECT \"c_0\".\"id\" FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"id\" = '1' );"
           (s/trim (eval/formatted-query (first (:queries (build "company | where: id = 1 | update! created_at = now()"))))))))
  (testing "On SQLite"
    (is (s/includes? (:query (first (:queries (build :test-sqlite "company | where: id = 1 | update! created_at = now()"))))
                     "SET \"created_at\" = CURRENT_TIMESTAMP"))))

;; -----
;; JOINS
;; -----

(deftest test-join-casts
  (testing "A cast stays with its side of the pair"
    (is (s/includes? (sql "employee | company .id = .company_id ::text") "ON \"e_0\".\"company_id\"::text = \"c_1\".\"id\""))
    (is (s/includes? (sql :test-mysql "employee | company .id ::text = .company_id") "ON `e_0`.`company_id` = CAST(`c_1`.`id` AS CHAR)"))
    (is (s/includes? (sql :test-sqlite "employee | company .id = .company_id ::text") "ON CAST(\"e_0\".\"company_id\" AS TEXT) = \"c_1\".\"id\""))))

;; -------------
;; ACCESS POLICY
;; -------------

(def ^:private integers-only [{:type "column-type" :allow ["integer"]}])

(deftest test-access-policy
  (testing "A value that reads a hidden column is hidden whole"
    (is (s/starts-with? (sql :test "report | s: coalesce(title, 'x') as k, title is null as untitled, coalesce(id, 0) as i, now() as t" integers-only)
                        "SELECT 'xxxxx' AS \"k\", 'xxxxx' AS \"untitled\", COALESCE(\"r_0\".\"id\", 0) AS \"i\", NOW() AS \"t\"")))

  (testing "So is an aggregate over one, or filtered by one"
    (is (s/includes? (sql :test "report | group: id => min(title) as m, count where title = 'a' as n, max(id)" integers-only)
                     "'xxxxx' AS \"m\", 'xxxxx' AS \"n\", MAX(\"x_1\".\"__pine_a1__\") AS \"max_id\"")))

  (testing "Its name can't be used in where:, order:, group: or after per"
    (doseq [step ["where: k = 'a'" "o: k desc" "l: 1 per k" "group: k"]]
      (is (= "`r_0.title` is hidden by the access policy, so it can't be used in where:, order:, group: or after `per`."
             (error :test (str "report | s: coalesce(title, 'x') as k | " step) integers-only))
          step))))

;; -----
;; /build
;; -----

(deftest test-build-ast
  (let [ast (:ast (api/api-build ["company as c | s: coalesce(c.id, 0) as k, id in (1, 2) as small | where: small = true | where: created_at > now()"] nil :test))]
    (testing "A computed column has its name, its text and the columns it uses"
      (is (= [{:computed true :column-alias "k" :alias "c" :operation-index 1
               :text "coalesce(c.id, 0)" :uses [{:alias "c" :column "id"}]}
              {:computed true :column-alias "small" :alias "c" :operation-index 1
               :text "c.id in (1, 2)" :uses [{:alias "c" :column "id"}]}]
             (filter :computed (:columns ast)))))
    (testing "A condition on a computed name, and a computed value on the right"
      (is (= {:computed true :text "c.id in (1, 2)" :column "small"}
             (select-keys (first (:where ast)) [:computed :text :column])))
      (is (= {:type :computed :text "now()" :uses []} (:value (second (:where ast))))))))

;; -----
;; HINTS
;; -----

(deftest test-hints
  (let [hints (fn [expression k] (map :column (get-in (api/api-build [expression] nil :test) [:ast :hints k])))]
    (testing "Computed names are offered where a later step can use them"
      (is (some #{"k"} (hints "company | s: coalesce(id, 0) as k | where: " :where)))
      (is (some #{"k"} (hints "company | s: coalesce(id, 0) as k | o: " :order)))
      (is (some #{"k"} (hints "company | s: coalesce(id, 0) as k | o: id desc | l: 1 per " :select)))
      (is (= ["k"] (filter #{"k"} (hints "company | s: coalesce(id, 0) as k | where: k" :where)))))
    (testing "But not in select:, where it is already selected"
      (is (not (some #{"k"} (hints "company | s: coalesce(id, 0) as k, " :select)))))))
