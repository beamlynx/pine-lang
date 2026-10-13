(ns pine.sql.review-test
  "Regression tests for the findings of the October 2026 review of SQL to
  Pine: each case is the reviewer's input. A conversion either means the
  same as the SQL, or is refused with a reason. See docs/sql-to-pine.md."
  (:require [clojure.string :as s]
            [clojure.test :refer [deftest is testing]]
            [pine.db.references :as refs]
            [pine.parser :as parser]
            [pine.sql.parser]
            [pine.sql.transpile :as t]))

(def ^:private schema
  (refs/index-references
   [[]
    [["public" "company" "id" 1 "integer" nil "NO" nil]
     ["public" "company" "name" 2 "text" nil "YES" nil]
     ["public" "company" "score" 3 "integer" nil "YES" nil]
     ["public" "company" "rn_order_1" 4 "integer" nil "YES" nil]
     ["public" "employee" "id" 1 "integer" nil "NO" nil]
     ["public" "employee" "company_id" 2 "integer" nil "YES" nil]
     ["public" "employee" "name" 3 "text" nil "YES" nil]
     ["public" "employee" "salary" 4 "integer" nil "YES" nil]
     ["public" "employee" "dept" 5 "text" nil "YES" nil]
     ["public" "k" "id" 1 "integer" nil "NO" nil]
     ["public" "k" "c" 2 "integer" nil "YES" nil]
     ["public" "k" "v" 3 "text" nil "YES" nil]
     ["public" "logs" "n" 1 "integer" nil "YES" nil]
     ["public" "logs" "v" 2 "text" nil "YES" nil]]
    [["public" "company" "id" 1]
     ["public" "employee" "id" 1]
     ["public" "k" "id" 1]]]))

(defn- ops [pine]
  (let [{:keys [result error]} (parser/parse pine)]
    (or result {:error error})))

(defn- blocks [pine]
  (mapv ops (s/split (s/trim pine) #"\n\s*\n")))

(defn- converts-to
  ([sql pine] (converts-to sql pine [] nil))
  ([sql pine warnings] (converts-to sql pine warnings nil))
  ([sql pine warnings references]
   (let [{:keys [expression error] :as r} (t/sql->pine sql references)]
     (is (nil? error) (str sql " => " error))
     (is (= (blocks pine) (blocks expression)) (str sql "\n  expected:\n" pine "\n  got:\n" expression))
     (is (= warnings (:warnings r)) sql))))

(defn- refuses
  ([sql message] (refuses sql message nil))
  ([sql message references]
   (let [{:keys [error error-type]} (t/sql->pine sql references)]
     (is (= "sql-unsupported" error-type) (str sql " => " error))
     (is (some-> error (s/includes? message)) (str sql " => " error)))))

(deftest test-silent-wrong-results
  (testing "A numbered WITH query read twice in one query is refused"
    (refuses "WITH r AS (SELECT e.id, e.company_id, ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY id) AS rn FROM employee e)
              SELECT b.id FROM r a JOIN r b ON b.company_id = a.company_id WHERE a.rn = 1"
             "this query reads it twice"))

  (testing "DISTINCT with LIMIT is never dropped"
    (refuses "UPDATE employee SET dept = 'z' WHERE company_id IN (SELECT DISTINCT c FROM k ORDER BY c LIMIT 2)"
             "DISTINCT with LIMIT isn't in Pine")
    (refuses "WITH w AS (SELECT DISTINCT c FROM k ORDER BY c LIMIT 2) SELECT id FROM employee WHERE company_id IN (SELECT c FROM w)"
             "DISTINCT isn't in Pine")
    (refuses "WITH w AS (SELECT DISTINCT c FROM k ORDER BY c LIMIT 2) SELECT e.id, (w.c IS NOT NULL) AS m FROM employee e LEFT JOIN w ON w.c = e.company_id"
             "DISTINCT isn't in Pine")
    (refuses "WITH w AS (SELECT DISTINCT c FROM k ORDER BY c LIMIT 2) SELECT e.id FROM employee e LEFT JOIN w ON w.c = e.company_id WHERE w.c IS NULL"
             "DISTINCT isn't in Pine"))

  (testing "The rows with no match: a WITH query with a LIMIT keeps its LIMIT before the NULL test"
    (converts-to "WITH w AS (SELECT c, v FROM k ORDER BY v DESC LIMIT 3) SELECT e.id FROM employee e LEFT JOIN w ON w.c = e.company_id WHERE w.c IS NULL"
                 "k | select: c, v | order: v desc | limit: 3 |= w

                  w | where: c is not null | select: c |= w_cs

                  employee as e | where: company_id is null or company_id not in w_cs | select: id"))

  (testing "Narrowed to one column, a WITH query loses an ORDER BY on a column it no longer selects"
    (converts-to "WITH w AS (SELECT c, COALESCE(v,'a') AS vv FROM k ORDER BY vv) SELECT e.id FROM employee e LEFT JOIN w ON w.c = e.company_id WHERE w.c IS NULL"
                 "k | where: c is not null | select: c |= w

                  employee as e | where: company_id is null or company_id not in w | select: id"))

  (testing "A cast to date that drops a time of day, and a time with a zone, are refused"
    (refuses "select id from employee where created_at < '2026-01-02 10:00'::date" "drops the time of day")
    (refuses "select id from employee where created_at < CAST('2026-01-02 10:00' AS date)" "drops the time of day")
    (refuses "select id from employee where created_at < '2026-01-02 10:00+05'::timestamptz" "has a time zone")
    (refuses "select id from employee where created_at < '2024-01-01T10:00:00Z'" "has a time zone")
    (converts-to "select id from employee where created_at < '2026-01-02'::date"
                 "employee | where: created_at < '2026-01-02' | select: id"))

  (testing "A column named like a Pine keyword is refused where Pine reads the keyword"
    (refuses "select id from employee where name = \"null\"" "reads as the keyword null")
    (refuses "select id from employee where name = \"true\"" "reads as the keyword true")
    (refuses "select coalesce(\"if\", name) as k from employee" "reads as the keyword if")
    (refuses "select coalesce(name, \"else\") as k from employee" "reads as the keyword else")
    (refuses "select \"null\" from employee" "reads as the keyword null")
    (testing "but not where Pine reads it as a column"
      (converts-to "select \"if\", \"then\", per, \"or\", count from t" "t | select: if, then, per, or, count")
      (converts-to "select * from t where \"if\" = 1 and a = \"if\"" "t | where: if = 1 | where: a = if")))

  (testing "CURRENT_TIMESTAMP and LOCALTIMESTAMP are now(); CURRENT_DATE and the like are refused by name"
    (converts-to "select id from employee where created_at < CURRENT_TIMESTAMP" "employee | where: created_at < now() | select: id")
    (converts-to "select id, LOCALTIMESTAMP as t from employee" "employee | select: id, now() as t")
    (converts-to "update employee set created_at = current_timestamp where id = 1" "employee | where: id = 1 | update! created_at = now()")
    (refuses "select id from employee where created_at < current_date" "CURRENT_DATE isn't in Pine")
    (refuses "select current_time as t from employee" "CURRENT_TIME isn't in Pine")
    (refuses "update employee set name = current_user where id = 1" "CURRENT_USER isn't in Pine"))

  (testing "A cast after an aggregate goes in the select:, or is refused"
    (converts-to "select company_id, max(salary)::text as m from employee group by company_id"
                 "employee | group: company_id => max(salary) as max_salary | select: company_id, max_salary::text as m")
    (converts-to "select a, cast(min(x) as text) as m from t group by a"
                 "t | group: a => min(x) as min_x | select: a, min_x::text as m")
    (refuses "select company_id, max(salary)::int as m from employee group by company_id" "A cast to int isn't in Pine"))

  (testing "A name Pine makes up for an IN set is never a table the SQL reads"
    (converts-to "select * from company_ids where id in (select id from company)"
                 "company | select: id |= company_ids_2

                  company_ids | where: id in company_ids_2"))

  (testing "A name Pine makes up for ROW_NUMBER()'s expression is never a column a * brings in"
    (converts-to "with r as (select c.*, row_number() over (partition by name order by (score is not null)) as rn from company c) select id from r where rn = 1"
                 "company as c | select: c.*, score is not null as rn_order_1_2 | order: rn_order_1_2 asc | limit: 1 per name |= r

                  r | select: id"
                 []
                 schema))

  (testing "Only Pine's own hidden column is left out, not an alias that looks like one"
    (converts-to "select id, name as __a__b from employee" "employee | select: id, name as __a__b")
    (converts-to "SELECT \"c_0\".\"id\" AS \"__c_0__id\", \"c_0\".* FROM \"company\" AS \"c_0\""
                 "company as c_0"
                 ["Left out __c_0__id: Pine adds its hidden columns itself."]))

  (testing "A WITH query named like a table the statement reads with its schema is refused"
    (refuses "with company as (select id, name from company where active) select c.name from public.company c join company a on a.id = c.id"
             "has the name of the table `public.company`")))

(deftest test-converts-then-fails
  (testing "Two aggregates with one name are refused"
    (refuses "select company_id, min(salary) as m, max(salary) as m from employee group by company_id"
             "Two selected columns are both called `m`"))

  (testing "Two columns compared with <, >, <= or >= are refused"
    (refuses "select e.id from employee e join company c on c.id = e.company_id where e.salary > c.score" "with > isn't in Pine")
    (refuses "select company_id from employee group by company_id having count(*) > min(salary)" "with > isn't in Pine")
    (refuses "select * from t where a > b" "with > isn't in Pine")
    (converts-to "select * from t where a != b" "t | where: a != b"))

  (testing "A string with a line break is refused"
    (refuses "select id from employee where name = 'a\n\nb'" "line break"))

  (testing "A bare column of a table the anti-join would remove keeps the join"
    (refuses "select e.id from employee e left join k on k.c = e.company_id where k.c is null and v = 'y'" "`v` could belong to")
    (testing "unless the schema says it isn't that table's"
      (converts-to "select e.id from employee e left join k on k.c = e.company_id where k.c is null and dept = 'y'"
                   "k | where: c is not null | select: c |= k_cs

                    employee as e | where: dept = 'y' | where: company_id is null or company_id not in k_cs | select: id"
                   []
                   schema)))

  (testing "With the schema, a subquery's bare column must be one of its own tables'"
    (refuses "select c.id from company c where c.id in (select company_id from employee where score > 5)"
             "`score` isn't a column of the subquery's table `employee`" schema))

  (testing "UPDATE of a table with no primary key is refused with the schema"
    (refuses "update logs set v = 'z' where v = 'a'" "`logs` has no primary key" schema))

  (testing "Deep nesting is a refusal, not a server error"
    (let [deep (str "select * from t where " (apply str (repeat 5000 "not ")) "a = 1")
          {:keys [expression error-type]} (t/sql->pine deep)]
      (is (or expression (= "sql-unsupported" error-type)) "converts or refuses, as the stack allows"))
    (with-redefs [pine.sql.parser/parse (fn [_] (throw (StackOverflowError.)))]
      (let [{:keys [error error-type]} (t/sql->pine "select * from t")]
        (is (= "sql-unsupported" error-type))
        (is (s/includes? (str error) "nested too deeply"))))
    (let [deep (str "select " (apply str (repeat 2000 "coalesce(")) "a" (apply str (repeat 2000 ", 1)")) " as k from t")
          {:keys [expression error-type]} (t/sql->pine deep)]
      (is (or expression (= "sql-unsupported" error-type)) "converts or refuses, as the stack allows"))))

(deftest test-converted-pine-is-built
  (testing "With a connection, each block is built as a check, and one that doesn't build is an internal error"
    (let [r (t/sql->pine "select * from company where id = 1" nil {:connection-id :test})]
      (is (nil? (:error r)) (:error r)))
    (with-redefs [pine.eval/build-query (fn [_] (throw (ex-info "boom" {})))]
      (let [r (t/sql->pine "select * from company where id = 1" nil {:connection-id :test})]
        (is (= "internal" (:error-type r)))
        (is (s/includes? (:error r) "doesn't build"))
        (is (s/includes? (:error r) "boom"))))))

(deftest test-messages-and-equivalents
  (testing "Each refusal names what was written"
    (refuses "with r as (select id, row_number() over (partition by a order by d.created_at + 1) as rn from t d) select * from r where rn = 1"
             "inside ROW_NUMBER() isn't in Pine: arithmetic isn't")
    (refuses "select * from t order by lower(b)" "ORDER BY lower(...) isn't in Pine: lower() isn't")
    (refuses "with w(a, b) as (select x, y from t) select * from w" "A column list after a WITH name")
    (refuses "SELECT 1; SELECT * FROM t" "A SELECT without FROM isn't in Pine")
    (refuses "select a, min(distinct x) as m from t group by a" "DISTINCT isn't in Pine")
    (refuses "select string_agg(a, ',') as s, b from t group by b" "Functions like string_agg()")
    (refuses "select (a between 1 and 2) as k from t" "BETWEEN inside a value")
    (refuses "select (a is not distinct from 1) as k from t" "IS [NOT] DISTINCT FROM inside a value")
    (refuses "select * from t where a = b + 1" "Arithmetic isn't in Pine"))

  (testing "What Pine can say converts"
    (converts-to "select a from t where a = coalesce(b, 1)" "t | where: a = coalesce(b, 1) | select: a")
    (converts-to "select * from t where a = b || 'x'" "t | where: a = concat(b, 'x')")
    (converts-to "update company set name = coalesce(name, 'x') where id = 1" "company | where: id = 1 | update! name = coalesce(name, 'x')")
    (converts-to "update company set name = name || 'x' where id = 1" "company | where: id = 1 | update! name = concat(name, 'x')")
    (converts-to "select a, sum(x) as s from t group by a order by sum(x)" "t | group: a => sum(x) as s | order: s asc")
    (converts-to "select coalesce(b, c) as k from t order by coalesce(b, c)" "t | select: coalesce(b, c) as k | order: k asc")
    (converts-to "select a, (a in (select b from k)) as m from t" "k | select: b |= k_bs\n\nt | select: a, a in k_bs as m")
    (converts-to "select x is not null as k from t" "t | select: x is not null as k")
    (converts-to "select a::text as k, cast(b as text) as l from t" "t | select: a::text as k, b::text as l")
    (converts-to "select (coalesce(a, b))::text as k from t" "t | select: coalesce(a, b)::text as k")
    (converts-to "select case x when 1 then 'a' else 'b' end as k from t" "t | select: if x = 1 then 'a' else 'b' as k")
    (converts-to "select b, count(*) filter (where a = 1) as n from t group by b" "t | group: b => count where a = 1 as n")
    (converts-to "select a, min(x) from t group by a" "t | group: a => min(x) as min"))

  (testing "Transaction words are named as written"
    (converts-to "BEGIN WORK; UPDATE company SET name = 'x' WHERE id = 1; END;"
                 "company | where: id = 1 | update! name = 'x'"
                 ["Left out BEGIN WORK and END. Pine runs each update! and delete! in a transaction of its own."]))

  (testing "The where: steps are counted in total, not per OR"
    (refuses (str "select * from t where " (s/join " and " (repeat 5 "((a=1 and b=1) or (c=1 and d=1))")))
             "more than 16 where: steps"))

  (testing "On MySQL, || is OR: refused. CONCAT() converts, since MySQL's is NULL when a part is"
    (let [mysql (fn [sql] (t/sql->pine sql nil {:dialect :mysql}))]
      (is (s/includes? (:error (mysql "select a || b as k from t")) "In MySQL, || means OR"))
      (is (= "t\n | select: concat(a, b) as k" (:expression (mysql "select concat(a, b) as k from t"))))))

  (testing "With no ORDER BY, which row is kept is said"
    (converts-to "with r as (select id, row_number() over (partition by a) as rn from t) select * from r where rn = 1"
                 "t | select: id | limit: 1 per a |= r\n\nr"
                 ["`rn` from ROW_NUMBER() isn't a column in Pine: `r` keeps the first rows of each group instead."
                  "ROW_NUMBER() has no ORDER BY, so the SQL keeps any row of each group. Pine keeps the first in primary key order."])))
