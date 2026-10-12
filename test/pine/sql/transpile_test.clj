(ns pine.sql.transpile-test
  (:require [clojure.string :as s]
            [clojure.test :refer [deftest is testing]]
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
                 ["Left out __c_0__id: Pine adds its hidden columns itself."])))

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

(defn- eval-sql [env {:keys [op args arg column]}]
  (case op
    :and (k-and (map #(eval-sql env %) args))
    :or (k-or (map #(eval-sql env %) args))
    :not (k-not (eval-sql env arg))
    :truthy (get env (:name column))))

(defn- eval-atom [env {:keys [col value]}]
  (let [v (get env (:name col))]
    (when-not (nil? v) (= v (:value value)))))

(defn- random-predicate [depth]
  (let [r (rand-int (if (zero? depth) 1 4))]
    (case r
      0 {:op :truthy :column {:name (rand-nth ["a" "b" "c" "d"])} :at 0}
      1 {:op :and :args (vec (repeatedly (+ 2 (rand-int 2)) #(random-predicate (dec depth))))}
      2 {:op :or :args (vec (repeatedly (+ 2 (rand-int 2)) #(random-predicate (dec depth))))}
      3 {:op :not :arg (random-predicate (dec depth)) :at 0})))

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
