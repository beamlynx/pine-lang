(ns pine.sql.transpile-test
  (:require [clojure.string :as s]
            [clojure.test :refer [deftest is testing]]
            [pine.api :as api]
            [pine.ast.main :as ast]
            [pine.eval :as eval]
            [pine.db.references :as refs]
            [pine.parser :as parser]
            [pine.sql.parser :as sp]
            [pine.sql.transpile :as t]))

(defn- ops
  "Pine text as parsed operations, so a comparison ignores layout."
  [pine]
  (let [{:keys [result error]} (parser/parse pine)]
    (or result {:error error})))

(defn- converts-to
  ([sql pine] (converts-to sql pine []))
  ([sql pine warnings]
   (let [{:keys [expression error] :as r} (t/sql->pine sql)]
     (is (nil? error) (str sql " => " error))
     (is (= (ops pine) (ops expression)) (str sql "\n  expected: " pine "\n  got:      " expression))
     (is (= warnings (:warnings r)) sql))))

(defn- blocks
  "Pine text split into its blocks, each as parsed operations."
  [pine]
  (mapv ops (s/split (s/trim pine) #"\n\s*\n")))

(defn- converts-to-blocks
  "Like converts-to, for a statement that becomes several blocks."
  ([sql pine] (converts-to-blocks sql pine [] nil))
  ([sql pine warnings] (converts-to-blocks sql pine warnings nil))
  ([sql pine warnings schema]
   (let [{:keys [expression error] :as r} (t/sql->pine sql schema)]
     (is (nil? error) (str sql " => " error))
     (is (= (blocks pine) (blocks expression)) (str sql "\n  expected:\n" pine "\n  got:\n" expression))
     (is (= warnings (:warnings r)) sql))))

(defn- refuses [sql message]
  (let [{:keys [error error-type]} (t/sql->pine sql)]
    (is (= "sql-unsupported" error-type) (str sql " => " error))
    (is (some-> error (s/includes? message)) (str sql " => " error))))

(deftest test-select
  (testing "Tables and columns"
    (converts-to "SELECT * FROM company" "company")
    (converts-to "select * from public.company as c" "public.company as c")
    (converts-to "select name, id from company" "company | select: name, id")
    (converts-to "select name as n from company" "company | select: name as n")
    (converts-to "select c.name from company c" "company as c | select: name")
    (converts-to "select c.* from company c" "company as c")
    (converts-to "select company.name from company" "company | select: name")
    (converts-to "select * from Company" "company")
    (converts-to "select * from \"Company\"" "Company"))

  (testing "Values"
    (converts-to "select * from t where a = 'O''Brien'" "t | where: a = 'O''Brien'")
    (converts-to "select * from t where a = -1.5 and b = true and c = $name" "t | where: a = -1.5 | where: b = true | where: c = $name")
    (converts-to "select * from t where a > '2024-01-01'" "t | where: a > '2024-01-01'")
    (converts-to "select * from t where a > '2024-01-01'::timestamp and b = 'x'::uuid" "t | where: a > '2024-01-01' | where: b = 'x'"))

  (testing "Comparisons"
    (converts-to "select * from t where a <> 1" "t | where: a != 1")
    (converts-to "select * from t where a >= 1 and a <= 9" "t | where: a >= 1 | where: a <= 9")
    (converts-to "select * from t where 5 < a" "t | where: a > 5")
    (converts-to "select * from t where a like 'x%' and b not ilike '%y'" "t | where: a like 'x%' | where: b not ilike '%y'")
    (converts-to "select * from t where a is null and b is not null" "t | where: a is null | where: b is not null")
    (converts-to "select * from t where a in ('x', 1) and b not in (2)" "t | where: a in ('x', 1) | where: b not in (2)")
    (converts-to "select * from t where a between 1 and 9" "t | where: a >= 1 | where: a <= 9")
    (converts-to "select * from t where a not between 1 and 9" "t | where: a < 1 or a > 9")
    (converts-to "select * from t where active" "t | where: active = true")
    (converts-to "select * from t where a::text = '1'" "t | where: a = '1' ::text")
    (converts-to "select * from t where cast(a as uuid) = 'x'" "t | where: a = 'x' ::uuid")
    (converts-to "select * from t where a = b" "t | where: a = b"))

  (testing "AND is separate where: steps, OR stays inside one"
    (converts-to "select * from t where a = 1 and (b = 2 or c = 3)" "t | where: a = 1 | where: b = 2 or c = 3")
    (converts-to "select * from t where (a = 1 and b = 2) or c = 3" "t | where: a = 1 or c = 3 | where: b = 2 or c = 3"))

  (testing "NOT is pushed down onto the comparisons"
    (converts-to "select * from t where not (a = 1 or b like 'x')" "t | where: a != 1 | where: b not like 'x'")
    (converts-to "select * from t where not (a > 1 and b is null)" "t | where: a <= 1 or b is not null")
    (converts-to "select * from t where not a in (1) and not not c = 2" "t | where: a not in (1) | where: c = 2")
    (converts-to "select * from t where not active" "t | where: active = false")
    (converts-to "select * from t where not (a not between 1 and 2)" "t | where: a >= 1 | where: a <= 2"))

  (testing "Order and limit. Pine sorts descending by default, so asc is written"
    (converts-to "select * from t order by a, b desc limit 5" "t | order: a asc, b desc | limit: 5")
    (converts-to "select name as n from t order by n" "t | select: name as n | order: name asc"))

  (testing "Count and group"
    (converts-to "select count(*) from t where a = 1" "t | where: a = 1 | count:")
    (converts-to "select status, count(*) from t group by status" "t | group: status => count")
    (converts-to "select status, count(*) as n from t group by status order by n desc"
                 "t | group: status => count | order: count desc"
                 ["COUNT(*) AS n: Pine names the count column `count`."])
    (converts-to "select status from t group by status order by status"
                 "t | group: status => count | order: status asc"
                 ["Pine adds a count column to a group:."])))

(deftest test-joins
  (testing "A join is the joined table's columns = the earlier table's"
    (converts-to "select * from company c join employee e on e.company_id = c.id"
                 "company as c | employee as e .company_id = .id | select: c.*, e.*")
    (converts-to "select e.* from company c join employee e on c.id = e.company_id"
                 "company as c | employee as e .company_id = .id")
    (converts-to "select c.name, e.name from company c left join employee e on e.company_id = c.id and e.x = c.y"
                 "company as c | employee as e :left .company_id = .id, .x = .y | select: c.name, name")
    (converts-to "select b.* from a right outer join b on b.a_id = a.id" "a | b :right .a_id = .id")
    (converts-to "select b.* from a join b using (a_id)" "a | b .a_id = .a_id"))

  (testing "A table without an alias gets one only when something refers to it"
    (converts-to "select company.name, employee.email from company join employee on employee.company_id = company.id"
                 "company as company | employee .company_id = .id | select: company.name, email"))

  (testing "A join tied to an earlier table than the last starts with from:"
    (converts-to "select * from company c join employee e on e.company_id = c.id join invoice i on i.company_id = c.id"
                 "company as c | employee as e .company_id = .id | from: c | invoice as i .company_id = .id | select: c.*, e.*, i.*"))

  (testing "An inner join's other ON conditions move to where:"
    (converts-to "select b.* from a join b on b.a_id = a.id and b.active = true"
                 "a | b .a_id = .id | where: active = true"
                 ["A condition in an inner join's ON moved to where:. It means the same there."]))

  (testing "Self join"
    (converts-to "select m.* from employee e join employee m on m.id = e.reports_to"
                 "employee as e | employee as m .id = .reports_to")))

;; A schema as pine.db.references indexes it: [schema table column ...].
(def ^:private schema
  (refs/index-references
   [[]
    (for [[sch table cols] [["public" "tenant" ["id" "name"]]
                            ["public" "company" ["id" "tenantId" "name"]]
                            ["public" "Invoice" ["id" "companyId"]]
                            ["public" "payment" ["id" "CompanyId"]]
                            ["Billing" "plan" ["id" "Name" "NAME"]]]
          [i col] (map-indexed vector cols)]
      [sch table col (inc i) "text" nil "YES" nil])
    []]))

(deftest test-database-spelling
  (let [with-schema (fn [sql] (t/sql->pine sql schema))]
    (testing "A bare name takes the database's spelling"
      (is (= "tenant as t\n | company as c .tenantId = .id\n | select: t.*, c.*"
             (:expression (with-schema "select * from tenant as t join company as c on t.id = c.tenantId"))))
      (is (= {:expression "Invoice\n | where: companyId = 1" :warnings []}
             (with-schema "select * from invoice where COMPANYID = 1")))
      (is (= {:expression "Invoice\n | select: id" :warnings []}
             (with-schema "select invoice.id from invoice"))))

    (testing "USING spells each side from its own table"
      (is (= "Invoice as invoice\n | payment as payment .CompanyId = .companyId\n | select: invoice.*, payment.*"
             (:expression (with-schema "select * from invoice join payment using (companyid)")))))

    (testing "A schema is spelled too"
      (is (= "Billing.plan\n | select: id" (:expression (with-schema "select id from billing.plan")))))

    (testing "The name as written, when the database has it or nothing like it"
      (is (= "company\n | select: name, nope" (:expression (with-schema "select name, nope from company")))))

    (testing "A quoted name keeps its spelling"
      (is (= "company\n | select: tenantid" (:expression (with-schema "select \"tenantid\" from company")))))

    (testing "Two names that differ only in case: the SQL can't say which"
      (is (= {:error "Line 1: `name` could be `NAME` or `Name`. Quote the one you mean, like \"NAME\"."
              :error-type "sql-unsupported"}
             (with-schema "select name from billing.plan"))))

    (testing "Without a schema, a bare name is folded to lowercase"
      (is (= "tenant as t\n | company as c .tenantid = .id\n | select: t.*, c.*"
             (:expression (t/sql->pine "select * from tenant as t join company as c on t.id = c.tenantId")))))))

(deftest test-pine-preview
  (testing "Pine's own SQL reads back, without its hidden columns"
    (converts-to "SELECT \"c_0\".\"id\" AS \"__c_0__id\", \"e_1\".* FROM \"company\" AS \"c_0\" JOIN \"employee\" AS \"e_1\" ON \"c_0\".\"id\" = \"e_1\".\"company_id\" LIMIT 250"
                 "company as c_0 | employee as e_1 .company_id = .id | limit: 250"
                 ["Left out __c_0__id: Pine adds its hidden columns itself."]))

  (testing "So does a preview with WITH, for a named result, count: or group:"
    (converts-to-blocks "WITH \"a\" AS ( SELECT \"c_0\".\"id\", \"c_0\".\"name\" FROM \"company\" AS \"c_0\" ) SELECT \"a\".* FROM \"a\" AS \"a\" WHERE \"a\".\"name\" = 'x' LIMIT 250"
                        "company as c_0 | select: id, name |= a

                         a as a | where: name = 'x' | limit: 250")
    (converts-to-blocks "WITH x AS ( SELECT \"c_0\".* FROM \"company\" AS \"c_0\" ) SELECT COUNT(*) FROM x"
                        "company as c_0 |= x

                         x | count:")
    (converts-to-blocks "WITH \"x_1\" AS ( SELECT \"c_0\".\"name\" AS \"name\" FROM \"company\" AS \"c_0\" ) SELECT \"x_1\".\"name\", COUNT(1) AS \"count\" FROM \"x_1\" GROUP BY \"x_1\".\"name\" LIMIT 10000"
                        "company as c_0 | select: name |= x_1

                         x_1 | group: name => count | limit: 10000")))

(deftest test-refusals
  (refuses "select * from t where a = null" "IS NULL")
  (refuses "select * from t where 1 = 1" "column on one side")
  (refuses "select * from t where a::int = 1" "cast to int")
  (refuses "select * from t where a = '1'::int" "cast to int")
  (refuses "select * from t where a like b" "string on its right")
  (refuses "select * from t where a in (true)" "strings and numbers")
  (refuses "select name from a join b on b.a_id = a.id" "could belong to `a` or `b`")
  (refuses "select x.name from a" "`x` isn't a table")
  (refuses "select * from a join b on b.x > a.y" "needs a column of `b`")
  (refuses "select * from a join b on b.a_id = a.id join c on c.a_id = a.id and c.b_id = b.id" "more than one table")
  (refuses "select * from a left join b on b.a_id = a.id and b.x = 1" "LEFT JOIN")
  (refuses "select * from a join b using (x) join c using (y)" "USING after more than one table")
  (refuses "select * from a join a on a.x = a.y" "Two tables are called `a`")
  (refuses "select * from \"my table\"" "can't be written as a name")
  (refuses "select * from t order by 1" "position")
  (refuses "select * from t order by count(*)" "needs GROUP BY")
  (refuses "select a, count(*) from t" "needs GROUP BY")
  (refuses "select count(*) from t limit 1" "LIMIT on a single COUNT(*)")
  (refuses "select a, b, count(*) from t group by a" "not grouped")
  (refuses "select * from t group by a" "Only grouped columns")
  (refuses "select * from t limit 100000" "more than Pine's")
  (refuses "select * from t where (a = 1 and b = 1) or (c = 1 and d = 1) or (e = 1 and f = 1) or (g = 1 and h = 1) or (i = 1 and j = 1)"
           "more than 16"))

(deftest test-select-refusals-that-moved
  (refuses "select distinct a from t" "DISTINCT isn't in Pine")
  (refuses "select * from t where a in (select id, name from u)" "must select one column")
  (refuses "select * from a join b on b.x = a.y::text" "A cast to text in a join's ON"))

(deftest test-error-lines
  (is (= {:error "Line 3: `= NULL` is never true in SQL. Write IS NULL or IS NOT NULL." :error-type "sql-unsupported"}
         (t/sql->pine "select *\nfrom t\nwhere a = null")))
  (is (= "sql-parse-error" (:error-type (t/sql->pine "company | where: id = 1")))))

;; -----------
;; FIXED POINT
;; -----------

(defn- pine->sql [expression]
  (-> expression parser/parse :result (ast/generate :test nil nil {} []) eval/build-query eval/formatted-query s/trim))

(def ^:private fixed-point-corpus
  "Pine expressions whose SQL converts back. Not count:, group: or a
  checkpoint: Pine writes those with WITH, which isn't converted yet."
  ["company"
   "company | where: id = 1"
   "company | where: name = 'Acme' | where: id > 3 or id < 1"
   "company | where: id >= 3 | where: id <= 9"
   "company | where: id in (1, 2) | where: name not in ('a', 'b')"
   "company | where: created_at > '2024-01-01'"
   "company | where: name like 'A%' or name is null"
   "company | employee"
   "company | employee | where: name ilike '%a%' | s: c_0.name, name"
   "company | employee :left | o: name asc | l: 5"
   "employee | document .created_by"
   "company as c | employee as e | from: c | document"
   "company | s: id, name as n | o: id desc"])

(deftest test-fixed-point
  (doseq [expression fixed-point-corpus]
    (testing expression
      (let [sql (pine->sql expression)
            {:keys [expression error]} (t/sql->pine sql)]
        (is (nil? error) (str sql " => " error))
        (when expression
          (is (= sql (pine->sql expression)) expression))))))

;; ----------------
;; NORMAL FORM, 3VL
;; ----------------
;;
;; A WHERE becomes a conjunction of disjunctions. That must hold under SQL's
;; three-valued logic, where a comparison with NULL is unknown: NOT unknown is
;; unknown, and a row is kept only when the whole condition is true.

(defn- k-and [xs] (cond (some false? xs) false (some nil? xs) nil :else true))
(defn- k-or [xs] (cond (some true? xs) true (some nil? xs) nil :else false))
(defn- k-not [x] (if (nil? x) nil (not x)))

(defn- eval-sql [env {:keys [op args arg column] :as p}]
  (case op
    :and (k-and (map #(eval-sql env %) args))
    :or (k-or (map #(eval-sql env %) args))
    :not (k-not (eval-sql env arg))
    :truthy (get env (:name column))
    ;; Never NULL: two NULLs aren't distinct, a NULL and a value are.
    :is-distinct (let [{:keys [negated left right]} p
                       distinct? (not= (get env (:name (:column left))) (:value right))]
                   (if negated (not distinct?) distinct?))))

(defn- eval-atom [env {:keys [col operator value]}]
  (let [v (get env (:name col))]
    (case operator
      "=" (when-not (nil? v) (= v (:value value)))
      "!=" (when-not (nil? v) (not= v (:value value)))
      "is null" (nil? v)
      "is not null" (some? v))))

(defn- random-predicate [depth]
  (case (rand-nth (if (zero? depth) [:truthy :distinct] [:truthy :distinct :and :or :not]))
    :truthy {:op :truthy :column {:name (rand-nth ["a" "b" "c" "d"])} :at 0}
    :distinct {:op :is-distinct :negated (rand-nth [true false]) :at 0
               :left {:kind :column :column {:name (rand-nth ["a" "b" "c" "d"])}}
               :right {:kind :boolean :value (rand-nth [true false])}}
    :and {:op :and :args (vec (repeatedly (+ 2 (rand-int 2)) #(random-predicate (dec depth))))}
    :or {:op :or :args (vec (repeatedly (+ 2 (rand-int 2)) #(random-predicate (dec depth))))}
    :not {:op :not :arg (random-predicate (dec depth)) :at 0}))

(deftest test-normal-form
  (let [tables [{:table "t"}]
        envs (for [a [true false nil] b [true false nil] c [true false nil] d [true false nil]]
               {"a" a "b" b "c" c "d" d})]
    (dotimes [_ 300]
      (let [p (random-predicate 3)
            clauses (try (#'t/plan-where tables [p]) (catch clojure.lang.ExceptionInfo _ nil))]
        (when clauses
          (doseq [env envs]
            (is (= (true? (eval-sql env p))
                   (true? (k-and (map (fn [clause] (k-or (map #(eval-atom env %) clause))) clauses))))
                (pr-str p env))))))))

(deftest test-reader-and-transpiler-agree-on-lines
  (is (= 2 (sp/line-of "a\nb" 2))))

;; ---------------------------------
;; WITH, SUBQUERIES, WRITES, SCRIPTS
;; ---------------------------------

;; A schema with nullability and primary keys, as pine.db.references indexes
;; it: [schema table column position type length nullable default].
(def ^:private keyed-schema
  (refs/index-references
   [[]
    [["public" "company_document" "id" 1 "uuid" nil "NO" nil]
     ["public" "company_document" "company_id" 2 "uuid" nil "NO" nil]
     ["public" "company_document" "reference_id" 3 "text" nil "YES" nil]
     ["public" "company_document" "created_at" 4 "timestamp" nil "NO" nil]
     ["public" "company_document" "archived_at" 5 "timestamp" nil "YES" nil]
     ["public" "company_document" "deleted_at" 6 "timestamp" nil "YES" nil]
     ["public" "company_document" "tenant_id" 7 "uuid" nil "NO" nil]
     ["public" "company_document_attachment" "company_document_id" 1 "uuid" nil "NO" nil]
     ["public" "company_document_attachment" "attachment_id" 2 "uuid" nil "NO" nil]
     ["public" "attachment" "id" 1 "uuid" nil "NO" nil]
     ["public" "attachment" "type" 2 "text" nil "YES" nil]
     ["public" "note" "id" 1 "integer" nil "NO" nil]
     ["public" "note" "body" 2 "text" nil "YES" nil]]
    [["public" "company_document" "id" 1]
     ["public" "attachment" "id" 1]]]))

(defn- runs-as-pine
  "Pine's SQL for the converted blocks, built the way a tab runs them, so a
  test sees the Pine is one Pine can build."
  [expression]
  (let [exprs (s/split expression #"\n\s*\n")
        {:keys [last-state]} (reduce (fn [{:keys [named-results]} e]
                                       (let [state (ast/generate (:result (parser/parse e)) :test nil nil named-results [])]
                                         {:named-results (merge named-results (:pending-assignments state))
                                          :last-state state}))
                                     {:named-results {}} exprs)]
    (eval/formatted-query (eval/build-query last-state))))

(deftest test-with
  (testing "Each WITH query is a block ending in |= name, then the statement's own block"
    (converts-to-blocks "with a as (select id, name from company where id > 1) select * from a where name = 'x'"
                        "company | where: id > 1 | select: id, name |= a

                         a | where: name = 'x'"))

  (testing "A WITH query can use an earlier one as a table"
    (converts-to-blocks "with a as (select * from company where id > 1), b as (select a.id from a where a.name = 'x') select * from b"
                        "company | where: id > 1 |= a

                         a | where: name = 'x' | select: id |= b

                         b"))

  (testing "A subquery in FROM or JOIN is a block named by its alias"
    (converts-to-blocks "select * from (select id from company where id > 1) x join employee e on e.company_id = x.id"
                        "company | where: id > 1 | select: id |= x

                         x as x | employee as e .company_id = .id | select: x.*, e.*"))

  (testing "A column of a WITH query keeps the spelling it was selected with"
    (is (= "company\n | select: tenantId |= a\n\na\n | where: tenantId = 1"
           (:expression (t/sql->pine "with a as (select tenantid from company) select * from a where tenantid = 1" schema)))))

  (testing "Refusals"
    (refuses "with a as (select * from t), a as (select * from u) select * from a" "Two WITH queries are called `a`")
    (refuses "with x as (select * from t) select * from (select * from u) x" "`x` names two queries")
    (refuses "with a as (select status, count(*) as n from t group by status) select * from a"
             "In the WITH query `a`: COUNT(*) AS n: Pine names the count column `count`.")
    (refuses "with a as (select status from t group by status) select * from a" "In the WITH query `a`: Pine adds a count column")))

(deftest test-top-n-per-group
  (testing "ROW_NUMBER() kept with rn = 1 is limit: 1 per"
    (converts-to-blocks "with r as (select t.*, row_number() over (partition by t.a order by t.b desc, t.c) as rn from t)
                         select id from r where rn = 1"
                        "t | order: b desc, c asc | limit: 1 per a |= r

                         r | select: id"))

  (testing "rn <= N and rn < N"
    (converts-to-blocks "with r as (select id, row_number() over (partition by a, b order by c) as rn from t) select * from r where rn <= 3"
                        "t | select: id | order: c asc | limit: 3 per a, b |= r

                         r"
                        ["`rn` from ROW_NUMBER() isn't a column in Pine: `r` keeps the first rows of each group instead."])
    (converts-to-blocks "with r as (select id, row_number() over (partition by a order by c) as rn from t) select * from r where 3 > rn and id > 1"
                        "t | select: id | order: c asc | limit: 2 per a |= r

                         r | where: id > 1"
                        ["`rn` from ROW_NUMBER() isn't a column in Pine: `r` keeps the first rows of each group instead."]))

  (testing "A subquery in FROM works the same"
    (converts-to-blocks "select x.id from (select t.*, row_number() over (partition by t.a order by t.b desc) as rn from t) x where x.rn = 1"
                        "t | order: b desc | limit: 1 per a |= x

                         x | select: id"))

  (testing "SELECT * over the numbered rows has no rn column in Pine"
    (converts-to-blocks "with r as (select id, row_number() over (partition by a order by c) as rn from t) select * from r where rn = 1"
                        "t | select: id | order: c asc | limit: 1 per a |= r

                         r"
                        ["`rn` from ROW_NUMBER() isn't a column in Pine: `r` keeps the first rows of each group instead."]))

  (testing "DISTINCT ON keeps the first row of each group, in the ORDER BY"
    (converts-to "select distinct on (a) a, b from t order by a, b desc" "t | select: a, b | order: a asc, b desc | limit: 1 per a")
    (converts-to "select distinct on (t.a, t.b) * from t order by b, a, c" "t | order: b asc, a asc, c asc | limit: 1 per a, b")
    (converts-to "select distinct on (a) * from t" "t | limit: 1 per a"))

  (testing "Refusals"
    (refuses "select id, row_number() over (partition by a order by b) as rn from t"
             "ROW_NUMBER() converts only when a later query keeps its first rows")
    (refuses "with r as (select id, row_number() over (partition by a order by b) as rn from t) select * from r"
             "`r` numbers rows with ROW_NUMBER(), so a query that uses it must keep its first rows, with WHERE rn = 1 or rn <= N.")
    (refuses "with r as (select id, row_number() over (partition by a order by b) as rn from t) select * from r where rn = 2"
             "WHERE rn = 2 keeps only row 2 of each group")
    (refuses "with r as (select id, row_number() over (partition by a order by b) as rn from t) select * from r where rn > 1"
             "WHERE rn > 1 keeps the rows after the first of each group, which isn't in Pine yet.")
    (refuses "with r as (select id, row_number() over (partition by a order by b) as rn from t) select id, rn from r where rn = 1"
             "`rn` comes from ROW_NUMBER()")
    (refuses "with r as (select id, row_number() over (partition by a order by b) as rn from t) select * from u left join r on r.id = u.id where r.rn = 1"
             "Joining it with LEFT or RIGHT JOIN isn't converted")
    (refuses "with r as (select id, row_number() over (order by b) as rn from t) select * from r where rn = 1"
             "ROW_NUMBER() without PARTITION BY")
    (refuses "with r as (select id, row_number() over (partition by a order by 1) as rn from t) select * from r where rn = 1"
             "ORDER BY a position inside ROW_NUMBER()")
    (refuses "with r as (select id, row_number() over (partition by a order by b) from t) select * from r"
             "ROW_NUMBER() needs a name")
    (refuses "with r as (select id, row_number() over (partition by a order by b) as rn from t), s as (select * from r where rn = 1) select * from r where rn <= 2"
             "keep different numbers of rows per group")
    (refuses "select distinct on (a) a, b from t order by b" "ORDER BY must start with the DISTINCT ON columns")
    (refuses "select distinct on (a) a, b from t order by a limit 5" "LIMIT after DISTINCT ON")))

(deftest test-in-a-named-set
  (testing "IN (SELECT c FROM cte): the WITH query selects only c"
    (converts-to-blocks "with k as (select id, name from company where name = 'x') select * from employee where company_id not in (select id from k)"
                        "company | where: name = 'x' | select: id |= k

                         employee | where: company_id not in k"))

  (testing "A WITH query used elsewhere too gets a block of its own for the IN"
    (converts-to-blocks "with k as (select id, name from company) select * from k where id in (select id from k where name = 'x')"
                        "company | select: id, name |= k

                         k | where: name = 'x' | select: id |= k_ids

                         k | where: id in k_ids"))

  (testing "A subquery on a table becomes a block"
    (converts-to-blocks "select * from employee where company_id in (select id from company where name like 'A%')"
                        "company | where: name like 'A%' | select: id |= company_ids

                         employee | where: company_id in company_ids"))

  (testing "DISTINCT is dropped from a set used only after IN"
    (converts-to-blocks "with k as (select distinct company_id from employee) select * from company where id in (select company_id from k)"
                        "employee | select: company_id |= k

                         company | where: id in k"))

  (testing "LEFT JOIN k ... WHERE k.c IS NULL keeps the rows with no k: not in"
    (converts-to-blocks "with k as (select id from note where body = 'x') select n.* from note n left join k on k.id = n.id where k.id is null"
                        "note | where: body = 'x' | select: id |= k

                         note as n | where: id not in k"
                        [] keyed-schema))

  (testing "Unless the columns are known NOT NULL, NULLs are handled the way the join does"
    (converts-to-blocks "with k as (select id from note where body = 'x') select n.* from note n left join k on k.id = n.id where k.id is null"
                        "note | where: body = 'x' | where: id is not null | select: id |= k

                         note as n | where: id is null or id not in k"))

  (testing "A table on the right of the LEFT JOIN gets a block of its own"
    (converts-to-blocks "select n.* from note n left join attachment a on a.id = n.id where a.id is null and n.body = 'x'"
                        "attachment | select: id |= attachment_ids

                         note as n | where: body = 'x' | where: id not in attachment_ids"
                        [] keyed-schema))

  (testing "A LEFT JOIN whose table is used elsewhere stays a join"
    (converts-to "select n.*, k.id from note n left join k on k.id = n.id where k.id is null"
                 "note as n | k :left .id = .id | where: id is null | select: n.*, id"))

  (testing "DISTINCT in a WITH query used as a table is still refused"
    (refuses "with k as (select distinct company_id from employee) select * from k" "DISTINCT isn't in Pine")))

(deftest test-is-distinct-from
  (converts-to "select * from t where a is distinct from 'x'" "t | where: a != 'x' or a is null")
  (converts-to "select * from t where a is not distinct from 'x'" "t | where: a = 'x' | where: a is not null")
  (converts-to "select * from t where not a is distinct from 'x'" "t | where: a = 'x' | where: a is not null")
  (converts-to "select * from t where a is distinct from null" "t | where: a is not null")
  (refuses "select * from t where a is distinct from b" "IS DISTINCT FROM another column"))

(deftest test-writes
  (testing "UPDATE is the rows it changes, then update!"
    (converts-to "update company set name = 'x', active = true, note = null where id = 1"
                 "company | where: id = 1 | update! name = 'x', active = true, note = null")
    (converts-to "update company set name = $name where id in (1, 2)"
                 "company | where: id in (1, 2) | update! name = $name"))

  (testing "UPDATE ... FROM joins on the WHERE conditions that tie the two"
    (converts-to "update employee e set name = 'x' from company c where e.company_id = c.id and c.name = 'Acme'"
                 "employee as e | company as c .id = .company_id | where: name = 'Acme' | from: e | update! name = 'x'"))

  (testing "DELETE names the rows by the table's primary key"
    (converts-to-blocks "delete from company_document where archived_at is not null"
                        "company_document | where: archived_at is not null | delete! .id"
                        [] keyed-schema)
    (converts-to-blocks "delete from company_document d using attachment a where a.id = d.id and a.type = 'x'"
                        "company_document as d | attachment as a .id = .id | where: type = 'x' | from: d | delete! .id"
                        [] keyed-schema))

  (testing "Refusals"
    (refuses "update company set updated_at = now() where id = 1" "NOW() isn't in Pine yet. update! writes a value")
    (refuses "update company set name = other where id = 1" "Setting a column from another column isn't converted")
    (refuses "update company set name = 'x'" "has no condition of its own")
    (refuses "update employee e set name = 'x' from company c where c.name = 'Acme'" "needs a WHERE condition that ties `c` to `e`")
    (refuses "delete from company where id = 1" "Converting a DELETE needs the connection")
    (is (= "Line 1: `note` has no primary key, so delete! can't name its rows."
           (:error (t/sql->pine "delete from note where id = 1" keyed-schema)))))

  (testing "Converting a write runs nothing, and an agent's read-only run refuses it"
    (let [{:keys [expression]} (t/sql->pine "update company set name = 'x' where id = 1")
          response (api/api-eval [expression] :test [] false)]
      (is (= "write-refused" (:error-type response)))
      (is (re-find #"read-only" (:error response))))))

(deftest test-converted-pine-builds
  ;; Each new shape, through Pine's AST and SQL, on the test fixtures' tables.
  (let [builds (fn [sql expected]
                 (let [{:keys [expression error]} (t/sql->pine sql)]
                   (is (nil? error) (str sql " => " error))
                   (when expression
                     (is (= expected (s/trim (runs-as-pine expression))) expression))))]
    (testing "select: before limit: N per, with order: and per columns that aren't selected"
      (builds "with r as (select id, row_number() over (partition by name order by created_at) as rn from company) select * from r where rn <= 3"
              (str "WITH \"r\" AS ( SELECT \"__pine_rows__\".\"id\" FROM ( SELECT \"c_0\".\"id\", "
                   "ROW_NUMBER() OVER (PARTITION BY \"c_0\".\"name\" ORDER BY \"c_0\".\"created_at\" ASC) AS \"__pine_row__\", "
                   "ROW_NUMBER() OVER (ORDER BY \"c_0\".\"created_at\" ASC) AS \"__pine_order__\" FROM \"company\" AS \"c_0\" ) AS \"__pine_rows__\" "
                   "WHERE \"__pine_rows__\".\"__pine_row__\" <= 3 ORDER BY \"__pine_rows__\".\"__pine_order__\" ) "
                   "SELECT \"r\".* FROM \"r\" AS \"r\" LIMIT 250;")))
    (testing "from: back to the changed table, then update!"
      (builds "update employee e set reports_to = 1 from company c where e.company_id = c.id and c.name = 'Acme'"
              (str "UPDATE \"employee\" SET \"reports_to\" = '1' WHERE \"id\" IN ( SELECT \"e\".\"id\" FROM \"employee\" AS \"e\" "
                   "JOIN \"company\" AS \"c\" ON \"e\".\"company_id\" = \"c\".\"id\" WHERE \"c\".\"name\" = 'Acme' );")))
    (testing "A subquery's block, joined by its alias"
      (builds "select * from (select id from company where id > 1) x join employee e on e.company_id = x.id"
              (str "WITH \"x\" AS ( SELECT \"c_0\".\"id\" FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"id\" > '1' ) "
                   "SELECT \"x\".*, \"e\".*, \"e\".\"id\" AS \"__e__id\" FROM \"x\" AS \"x\" JOIN \"employee\" AS \"e\" ON \"x\".\"id\" = \"e\".\"company_id\" LIMIT 250;")))
    (testing "A set block, then in"
      (builds "select * from employee where company_id in (select id from company where name like 'A%')"
              (str "WITH \"company_ids\" AS ( SELECT \"c_0\".\"id\" FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"name\" LIKE 'A%' ) "
                   "SELECT \"e_0\".\"id\" AS \"__e_0__id\", \"e_0\".* FROM \"employee\" AS \"e_0\" "
                   "WHERE \"e_0\".\"company_id\" IN ( SELECT \"id\" FROM \"company_ids\" ) LIMIT 250;")))
    (testing "Rows with no match, with the NULL guards"
      (builds "with k as (select id from company where name = 'x') select e.* from employee e left join k on k.id = e.company_id where k.id is null"
              (str "WITH \"k\" AS ( SELECT \"c_0\".\"id\" FROM \"company\" AS \"c_0\" WHERE \"c_0\".\"name\" = 'x' AND \"c_0\".\"id\" IS NOT NULL ) "
                   "SELECT \"e\".\"id\" AS \"__e__id\", \"e\".* FROM \"employee\" AS \"e\" "
                   "WHERE (\"e\".\"company_id\" IS NULL OR \"e\".\"company_id\" NOT IN ( SELECT \"id\" FROM \"k\" )) LIMIT 250;")))))

(deftest test-scripts
  (testing "BEGIN and COMMIT are dropped, with a warning"
    (converts-to "BEGIN;\nupdate company set name = 'x' where id = 1;\nCOMMIT;"
                 "company | where: id = 1 | update! name = 'x'"
                 ["Left out BEGIN and COMMIT. Pine runs each update! and delete! in a transaction of its own."]))

  (testing "The statement that changes data converts; the rest are left out, by line"
    (converts-to "select * from company where id = 1;\nupdate company set name = 'x' where id = 1;\nselect count(*) from company where name = 'x';"
                 "company | where: id = 1 | update! name = 'x'"
                 ["Left out the statement on line 1. Pine converts one statement at a time."
                  "Left out the statement on line 3. Pine converts one statement at a time."]))

  (testing "Without a write, the first statement converts"
    (converts-to "select * from company; select * from employee"
                 "company"
                 ["Left out the statement on line 1. Pine converts one statement at a time."]))

  (testing "Refusals"
    (refuses "update a set x = 1 where id = 1; delete from b where id = 2" "changes data in more than one statement")
    (is (= "Line 3: The script ends in ROLLBACK, which undoes its change. Pine's update! and delete! keep theirs, so it doesn't convert."
           (:error (t/sql->pine "begin;\nupdate a set x = 1 where id = 1;\nrollback;"))))
    (refuses "BEGIN; COMMIT;" "only BEGIN, COMMIT or ROLLBACK")
    (refuses "call refresh_all(); update a set x = 1 where id = 1" "changes data in more than one statement")
    (is (= "Line 1: DO doesn't convert to Pine. Only SELECT, UPDATE and DELETE do."
           (:error (t/sql->pine "DO $$ BEGIN UPDATE t SET a = 1; END $$; select * from t"))))
    (refuses "update a set x = 1 where id = 1; explain analyze update b set y = 2" "changes data in more than one statement")
    (is (= "Line 2: INSERT doesn't convert to Pine. Only SELECT, UPDATE and DELETE do."
           (:error (t/sql->pine "select 1 from t;\ninsert into t values (1)"))))))

;; ------------------
;; THE MOTIVATING ONE
;; ------------------
;;
;; An agent's script that soft-deletes duplicate SharePoint documents. Parts
;; of it aren't in Pine yet, so it is refused where the first one is.

(def ^:private motivating-sql
  "BEGIN;
WITH sp_docs AS (
    SELECT d.id, d.company_id, d.reference_id, d.created_at, d.archived_at,
        COALESCE(MIN(a.id::text) FILTER (WHERE a.type = 'sharepoint'), MIN(a.id::text)) AS any_attachment_id
    FROM company_document d
    LEFT JOIN company_document_attachment cda ON cda.company_document_id = d.id
    LEFT JOIN attachment a ON a.id = cda.attachment_id
    WHERE d.tenant_id = '86b8ad91-7463-478a-aa43-43a977105bf4' AND d.deleted_at IS NULL
    GROUP BY d.id, d.company_id, d.reference_id, d.created_at, d.archived_at
    HAVING BOOL_OR(a.type = 'sharepoint') OR d.reference_id IS NOT NULL
),
grouped AS (SELECT s.*, COALESCE(s.reference_id, 'att:' || s.any_attachment_id) AS dedupe_key FROM sp_docs s),
edited AS (
    SELECT DISTINCT g.id AS document_id FROM grouped g
    JOIN activity_resource ar ON ar.\"resourceableId\" = g.id::text
    JOIN activity act ON act.id = ar.\"activityId\"
    WHERE act.\"tenantId\" = '86b8ad91-7463-478a-aa43-43a977105bf4'
      AND act.\"userId\" IS DISTINCT FROM 'd3b93066-ea59-4259-a534-943249fba79c'
      AND act.\"type\" IS DISTINCT FROM 'company-document-downloaded'
),
ranked AS (
    SELECT g.*, (e.document_id IS NOT NULL) AS has_activity,
        ROW_NUMBER() OVER (PARTITION BY g.company_id, g.dedupe_key
            ORDER BY (e.document_id IS NOT NULL) DESC, CASE WHEN e.document_id IS NOT NULL THEN g.created_at END DESC, g.created_at DESC, g.id ASC) AS rn
    FROM grouped g LEFT JOIN edited e ON e.document_id = g.id WHERE g.archived_at IS NULL
),
keep_final AS (SELECT id, has_activity FROM ranked WHERE rn = 1)
UPDATE company_document d SET deleted_at = NOW()
FROM grouped g LEFT JOIN keep_final kf ON kf.id = g.id
WHERE d.id = g.id AND kf.id IS NULL AND g.archived_at IS NULL;
COMMIT;
")

(def ^:private motivating-ranked-by-expression
  "The same, without COALESCE, FILTER, HAVING and has_activity, but still
  ranking by an expression."
  "BEGIN;
WITH sp_docs AS (
    SELECT d.id, d.company_id, d.reference_id, d.created_at, d.archived_at
    FROM company_document d
    LEFT JOIN company_document_attachment cda ON cda.company_document_id = d.id
    LEFT JOIN attachment a ON a.id = cda.attachment_id
    WHERE d.tenant_id = '86b8ad91-7463-478a-aa43-43a977105bf4' AND d.deleted_at IS NULL
      AND (a.type = 'sharepoint' OR d.reference_id IS NOT NULL)
),
edited AS (
    SELECT DISTINCT g.id AS document_id FROM sp_docs g
    JOIN activity_resource ar ON ar.\"resourceableId\" = g.id
    JOIN activity act ON act.id = ar.\"activityId\"
    WHERE act.\"tenantId\" = '86b8ad91-7463-478a-aa43-43a977105bf4'
      AND act.\"userId\" IS DISTINCT FROM 'd3b93066-ea59-4259-a534-943249fba79c'
      AND act.\"type\" IS DISTINCT FROM 'company-document-downloaded'
),
ranked AS (
    SELECT g.*,
        ROW_NUMBER() OVER (PARTITION BY g.company_id, g.reference_id
            ORDER BY (e.document_id IS NOT NULL) DESC, g.created_at DESC, g.id ASC) AS rn
    FROM sp_docs g LEFT JOIN edited e ON e.document_id = g.id WHERE g.archived_at IS NULL
),
keep_final AS (SELECT id FROM ranked WHERE rn = 1)
UPDATE company_document d SET deleted_at = '2026-10-12'
FROM sp_docs g LEFT JOIN keep_final kf ON kf.id = g.id
WHERE d.id = g.id AND kf.id IS NULL AND g.archived_at IS NULL;
COMMIT;
")

(def ^:private motivating-reduced
  "The structure alone: no GROUP BY dedupe, no activity ranking, and a
  literal for NOW()."
  "BEGIN;
WITH sp_docs AS (
    SELECT d.id, d.company_id, d.reference_id, d.created_at, d.archived_at
    FROM company_document d
    LEFT JOIN company_document_attachment cda ON cda.company_document_id = d.id
    LEFT JOIN attachment a ON a.id = cda.attachment_id
    WHERE d.tenant_id = '86b8ad91-7463-478a-aa43-43a977105bf4' AND d.deleted_at IS NULL
      AND (a.type = 'sharepoint' OR d.reference_id IS NOT NULL)
),
ranked AS (
    SELECT g.*,
        ROW_NUMBER() OVER (PARTITION BY g.company_id, g.reference_id ORDER BY g.created_at DESC, g.id ASC) AS rn
    FROM sp_docs g WHERE g.archived_at IS NULL
),
keep_final AS (SELECT id FROM ranked WHERE rn = 1)
UPDATE company_document d SET deleted_at = '2026-10-12'
FROM sp_docs g LEFT JOIN keep_final kf ON kf.id = g.id
WHERE d.id = g.id AND kf.id IS NULL AND g.archived_at IS NULL;
COMMIT;
")

(deftest test-motivating-script
  (testing "The first thing Pine can't say, with its line"
    (is (= {:error "Line 4: Functions like coalesce() aren't in Pine." :error-type "sql-unsupported"}
           (t/sql->pine motivating-sql keyed-schema))))

  (testing "Without those, ranking by an expression is next"
    (is (= {:error "Line 21: ORDER BY (e.document_id IS NOT NULL) inside ROW_NUMBER() isn't in Pine yet. Pine ranks rows by columns."
            :error-type "sql-unsupported"}
           (t/sql->pine motivating-ranked-by-expression keyed-schema))))

  (testing "The structure converts, close to the Pine a person would write"
    (converts-to-blocks motivating-reduced
                        "company_document as d
                          | company_document_attachment as cda :left .company_document_id = .id
                          | attachment as a :left .id = .attachment_id
                          | where: d.tenant_id = '86b8ad91-7463-478a-aa43-43a977105bf4'
                          | where: d.deleted_at is null
                          | where: type = 'sharepoint' or d.reference_id is not null
                          | select: d.id, d.company_id, d.reference_id, d.created_at, d.archived_at |= sp_docs

                         sp_docs as g | where: archived_at is null | order: created_at desc, id asc | limit: 1 per company_id, reference_id |= ranked

                         ranked | select: id |= keep_final

                         company_document as d
                          | sp_docs as g .id = .id
                          | where: archived_at is null
                          | where: id not in keep_final
                          | from: d
                          | update! deleted_at = '2026-10-12'"
                        ["Left out BEGIN and COMMIT. Pine runs each update! and delete! in a transaction of its own."]
                        keyed-schema))

  (testing "Without the schema, NULLs are guarded, since nothing says the ids are NOT NULL"
    (let [{:keys [expression]} (t/sql->pine motivating-reduced)]
      (is (s/includes? expression "ranked\n | where: id is not null\n | select: id |= keep_final"))
      (is (s/includes? expression "| where: id is null or id not in keep_final"))))

  (testing "Pine builds the rows it changes (the test fixtures have no company_document key for update! itself)"
    (let [expression (:expression (t/sql->pine motivating-reduced keyed-schema))
          sql (runs-as-pine (s/replace expression "\n | from: d\n | update! deleted_at = '2026-10-12'" ""))]
      (is (s/includes? expression "\n | from: d\n | update! deleted_at = '2026-10-12'"))
      (is (s/includes? sql "NOT IN ( SELECT \"id\" FROM \"keep_final\" )"))
      (is (s/includes? sql "PARTITION BY \"g\".\"company_id\", \"g\".\"reference_id\" ORDER BY \"g\".\"created_at\" DESC, \"g\".\"id\" ASC")))))
