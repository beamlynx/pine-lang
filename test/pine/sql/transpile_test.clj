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

  (testing "A key inside a JSON value is a Pine path. `->` is shown as text, with a warning"
    (converts-to "select t.data->>'plan' as p, t.data->'a b'->>'c' as ab, t.data->'tags'->>0 as tag from t"
                 "t | select: data.plan as p, data.'a b'.c as ab, data.tags[0] as tag")
    (converts-to "select t.data->'plan' as p from t" "t | select: data.plan as p"
                 ["In SQL, `->` gives JSON, so a string keeps its quotes. Pine shows data.plan as text, without them, as `->>` does."])
    (converts-to "select c.data->>'k' as k from c join o on o.cid = c.id" "c as c | o .cid = .id | select: c.data.k as k | from: c"))

  (testing "A first key after a bare name that is also an alias is quoted, so Pine reads a key"
    (converts-to "select data.x->>'k' as k from t as data" "t as data | select: x.k as k")
    (converts-to "select t.data->>'k' as k from t join data on data.tid = t.id"
                 "t as t | data .tid = .id | select: t.data.k as k | from: t")
    (converts-to "select data.data->>'k' as k from t join data on data.tid = t.id"
                 "t | data .tid = .id | select: data.'k' as k"))

  (testing "jsonb_typeof() and jsonb_array_length() keep their names, and read a key as JSON: no warning"
    (converts-to "select jsonb_typeof(t.data->'a') as k, jsonb_array_length(t.data) as n from t"
                 "t | select: jsonb_typeof(data.a) as k, jsonb_array_length(data) as n")
    (converts-to "select jsonb_array_length(case when jsonb_typeof(t.d) = 'array' then t.d else t.d->'nodes' end) as n from t"
                 "t | select: jsonb_array_length(if jsonb_typeof(d) = 'array' then d else d.nodes) as n"))

  (testing "A function on the left of a condition"
    (converts-to "select * from t where jsonb_typeof(t.d) = 'object' and 'array' = jsonb_typeof(t.d->'tags')"
                 "t | where: jsonb_typeof(d) = 'object' | where: jsonb_typeof(d.tags) = 'array'")
    (converts-to "select * from t where not jsonb_typeof(t.d) = 'object'" "t | where: jsonb_typeof(d) != 'object'")
    (converts-to "select * from t where coalesce(t.a, t.b) = 'x'" "t | where: coalesce(a, b) = 'x'")
    (converts-to "select * from t where t.x = coalesce(t.a, 'y')" "t | where: x = coalesce(a, 'y')"))

  (testing "On MySQL and SQLite, a key that starts with $ is a path. On Postgres it is a key"
    (is (= "t\n | select: data.plan as p, data.a.'b c'[2] as q"
           (:expression (t/sql->pine "select t.data->>'$.plan' as p, t.data->>'$.a.\"b c\"[2]' as q from t" nil {:dialect :mysql}))))
    (is (= "t\n | select: data.'$.plan' as p"
           (:expression (t/sql->pine "select t.data->>'$.plan' as p from t" nil {:dialect :postgres}))))
    (refuses "select t.data->>'$.plan' as p from t" "Convert with the connection")
    (is (s/includes? (:error (t/sql->pine "select t.data->>'$.*' as p from t" nil {:dialect :sqlite})) "isn't converted. Pine follows keys and indexes")))

  (testing "ORDER BY a -> key, or its name, sorts by the path: both sort JSON"
    (converts-to "select t.data->'plan' as p from t order by p desc nulls last" "t | select: data.plan as p | order: data.plan desc nulls last"
                 ["In SQL, `->` gives JSON, so a string keeps its quotes. Pine shows data.plan as text, without them, as `->>` does."])
    (converts-to "select t.id from t order by t.data->'plan'" "t | select: id | order: data.plan asc")
    (converts-to "select c.id from c join o on o.cid = c.id order by c.data->'k'" "c as c | o .cid = .id | select: c.id | order: c.data.k asc | from: c"))

  (testing "A CASE or COALESCE of -> keys and literals is shown as text, with the warning"
    (converts-to "select coalesce(t.d->'a', t.d->'b') as x from t" "t | select: coalesce(d.a, d.b) as x"
                 ["In SQL, `->` gives JSON, so a string keeps its quotes. Pine shows d.a as text, without them, as `->>` does."
                  "In SQL, `->` gives JSON, so a string keeps its quotes. Pine shows d.b as text, without them, as `->>` does."])
    (converts-to "select coalesce(t.d->>'a', t.name) as x from t" "t | select: coalesce(d.a, name) as x"))

  (testing "A ->> key in WHERE compares its text, where Pine's comparison means the same"
    (converts-to "select * from t where t.data->>'plan' = 'pro'" "t | where: data.plan = 'pro'")
    (converts-to "select * from t where 'pro' = t.data->>'plan'" "t | where: data.plan = 'pro'")
    ;; ->> gives NULL for a JSON null, which SQL's != drops and Pine's would keep.
    (converts-to "select * from t where not (t.data->>'plan' = 'pro')" "t | where: data.plan != 'pro' | where: data.plan is not null")
    (is (= "t\n | where: data.plan != 'pro'" (:expression (t/sql->pine "select * from t where t.data->>'$.plan' != 'pro'" nil {:dialect :mysql}))))
    (is (re-find #"IS NULL on a ->> key isn't converted on MySQL"
                 (:error (t/sql->pine "select * from t where t.data->>'$.plan' is null" nil {:dialect :mysql}))))
    (converts-to "select * from t where t.data->>'plan' is null" "t | where: data.plan is null")
    (converts-to "select * from t where t.data->>'plan' like 'p%'" "t | where: data.plan like 'p%'")
    (converts-to "select * from t where t.data->>'plan' not in ('a', 'b')" "t | where: data.plan not in ('a', 'b')"))

  (testing "and refuses where it doesn't"
    (refuses "select * from t where t.data->>'n' = '10'" "'10' is also how a JSON number reads as text")
    (refuses "select * from t where t.data->>'n' = 'null'" "'null' is also how a JSON null reads as text")
    (refuses "select * from t where t.data->>'n' = '{}'" "'{}' is also how a JSON object or array reads as text")
    (refuses "select * from t where t.data->>'n' = 10" "The text of a JSON key is compared with a string")
    (refuses "select * from t where t.data->>'n' in ('a', 1)" "The text of a JSON key is compared with strings")
    (refuses "select * from t where t.data->>'n' > 'a'" "Comparing the text of a JSON key with > isn't converted")
    (refuses "select * from t where t.data->'n' = '\"a\"'" "A -> key here is JSON")
    (refuses "select * from t where (t.data->>'n')::int = 1" "A cast on a JSON key isn't converted here"))

  (testing "A JSON key elsewhere, or where Pine would read it as text, is refused"
    (refuses "select jsonb_array_length(coalesce(t.d->'a', t.d->'b')) as n from t" "A JSON key inside this function isn't converted")
    (refuses "select t.x->-1 as y from t" "A JSON index converts when it is a whole number")
    (refuses "select * from t where jsonb_typeof(t.d) = t.kind" "Functions like jsonb_typeof() aren't in Pine here")
    (refuses "select t.data->>'plan' as p from t order by p" "ORDER BY a ->> key isn't converted")
    (refuses "select t.data->>'plan' as p, count(*) from t group by p" "isn't converted in a query with GROUP BY")
    ;; Pine would show the key as text and the column as JSON: Postgres
    ;; can't put both in one column.
    (refuses "select case when t.a = 1 then t.d else t.d->'nodes' end as x from t" "A CASE that mixes a `->` key with another value")
    (refuses "select coalesce(t.d->'a', t.d) as x from t" "A COALESCE that mixes a `->` key with another value"))

  (testing "NULLS FIRST and NULLS LAST"
    (converts-to "select * from t order by a nulls first, b desc nulls last" "t | order: a asc nulls first, b desc nulls last")
    (converts-to "select a, count(*) from t group by a order by a nulls first" "t | group: a with count | order: a asc nulls first")
    (converts-to-blocks "select * from (select *, row_number() over (partition by a order by b nulls last) as rn from t) x where rn = 1"
                        "t | order: b asc nulls last | limit: 1 per a |= x\n\nx"
                        ["`rn` from ROW_NUMBER() isn't a column in Pine: `x` keeps the first rows of each group instead."]))

  (testing "Count and group"
    (converts-to "select count(*) from t where a = 1" "t | where: a = 1 | count:")
    (converts-to "select status, count(*) from t group by status" "t | group: status with count")
    (converts-to "select status, count(*) as n from t group by status order by n desc"
                 "t | group: status with count as n | order: n desc")
    (testing "Pine's count column is left out when the SQL has none"
      (converts-to "select status from t group by status order by status"
                   "t | group: status | select: status | order: status asc")
      (converts-to "select count(*) from t group by status" "t | group: status with count | select: count"))
    (testing "A grouped column keeps the SQL's name"
      (converts-to "select status as s, count(*) from t group by status order by s"
                   "t | group: status with count | select: status as s, count | order: s asc"))
    (testing "ORDER BY a grouped column the SELECT leaves out sorts before the select:"
      (converts-to "select count(*) as n from t group by status order by status"
                   "t | group: status with count as n | order: status asc | select: n"))
    (testing "Two SQL columns of one name: the second is renamed, with a warning"
      (converts-to "select count(*), min(score) as count from t group by a"
                   "t | group: a with count as count_2, score => min as count | select: count_2, count"
                   ["`count_2` is the SQL's column `count`: Pine needs each name once."]))))

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

    (testing "An unqualified column belongs to the one table that has it"
      (is (= "tenant as t\n | company as company .tenantId = .id\n | select: t.*, company.*"
             (:expression (with-schema "select * from tenant as t join company on t.id = tenantId"))))
      (is (= {:error "Line 1: `name` could belong to `t` or `c`. Qualify it, like `t.name`."
              :error-type "sql-unsupported"}
             (with-schema "select name from tenant t join company c on c.tenantId = t.id")))
      (is (= {:error "Line 1: `nope` isn't a column of `t` or `c`."
              :error-type "sql-unsupported"}
             (with-schema "select nope from tenant t join company c on c.tenantId = t.id")))
      (is (= {:error "Line 1: `tenantid` could belong to `t` or `o`. Qualify it, like `t.tenantid`."
              :error-type "sql-unsupported"}
             (with-schema "select * from tenant t join other o on o.tenantId = t.id where tenantId = 1"))
          "A table the schema doesn't have could have the column too"))

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

                         x_1 | group: name with count | limit: 10000")))

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
  (refuses "select * from t order by 1" "ORDER BY 1 counts past a `*`")
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
  (testing "A cast in a join's ON stays with its column"
    (converts-to "select b.* from a join b on b.x = a.y::text" "a | b .x = .y ::text")
    (converts-to "select b.* from a join b on b.x::uuid = a.y" "a | b .x ::uuid = .y")
    (refuses "select b.* from a join b on b.x = a.y::int" "cast to int")))

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
  "Pine expressions whose SQL converts back to Pine that builds the same SQL.
  count:, group: and checkpoints are written with WITH, which converts to
  several blocks: see multi-block-corpus."
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
   "company | s: id, name as n | o: id desc"
   ;; Computed values.
   "company | s: coalesce(name, 'x') as k"
   "company | s: concat(name, 'x') as k"
   "company | s: if id = 1 then 'a' else 'b' as k"
   "company | s: id = 1 or name is null as k"
   "company | s: id, name::text as k"
   "company | s: now() as t"
   "company | where: name = coalesce(name, 'x')"
   "company | where: name = concat(name, 'x')"
   "company | where: created_at < now()"])

(def ^:private multi-block-corpus
  "Pine expressions whose SQL has WITH: it converts back, to several blocks."
  ["company | group: name with count"
   "company | group: name with count as n"
   "company | group: name with id => min as m"
   "company | group: name with count where id > 1 as n"
   "company | group: name with count | where: count > 1"
   "company | where: id = 1 | count:"])

(deftest test-multi-block-round-trip
  (doseq [expression multi-block-corpus]
    (testing expression
      (let [{:keys [error]} (t/sql->pine (pine->sql expression) nil {:connection-id :test})]
        (is (nil? error) error)))))

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
    (refuses "with a as (select count(*), min(x) as count from t group by status) select * from a"
             "In the WITH query `a`: `count_2` is the SQL's column `count`"))

  (testing "A GROUP BY in a WITH query has exactly the SQL's columns"
    (converts-to-blocks "with a as (select status, count(*) as n from t group by status) select * from a"
                        "t | group: status with count as n |= a\n\na")
    (converts-to-blocks "with a as (select status from t group by status) select * from a"
                        "t | group: status | select: status |= a\n\na")))

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
    (converts-to "select distinct on (a) * from t" "t | limit: 1 per a"
                 ["DISTINCT ON has no ORDER BY, so the SQL keeps any row of each group. Pine keeps the first in primary key order."]))

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
    (converts-to "update company set updated_at = now() where id = 1" "company | where: id = 1 | update! updated_at = now()")
    (refuses "update company set name = lower(name) where id = 1" "lower() isn't in Pine's update!. update! writes a value")
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
                   "\"c_0\".\"name\" AS \"__pine_key_1__\", "
                   "ROW_NUMBER() OVER (PARTITION BY \"c_0\".\"name\" ORDER BY \"c_0\".\"created_at\" ASC) AS \"__pine_row__\" FROM \"company\" AS \"c_0\" ) AS \"__pine_rows__\" "
                   "WHERE \"__pine_rows__\".\"__pine_row__\" <= 3 ORDER BY \"__pine_rows__\".\"__pine_key_1__\", \"__pine_rows__\".\"__pine_row__\" ) "
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
;; An agent's script that soft-deletes duplicate SharePoint documents. It
;; converts in full.

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

(def ^:private motivating-script
  "The script exactly as the agent wrote it: with a ROW_NUMBER() it never
  uses, and a check after the UPDATE."
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
            ORDER BY (e.document_id IS NOT NULL) DESC, CASE WHEN e.document_id IS NOT NULL THEN g.created_at END DESC, g.created_at DESC, g.id ASC) AS rn,
        ROW_NUMBER() OVER (PARTITION BY g.company_id, g.dedupe_key ORDER BY g.created_at ASC, g.id ASC) AS rn_oldest_only
    FROM grouped g LEFT JOIN edited e ON e.document_id = g.id WHERE g.archived_at IS NULL
),
keep_final AS (SELECT id, has_activity FROM ranked WHERE rn = 1)
UPDATE company_document d SET deleted_at = NOW()
FROM grouped g LEFT JOIN keep_final kf ON kf.id = g.id
WHERE d.id = g.id AND kf.id IS NULL AND g.archived_at IS NULL;
SELECT COUNT(*) FROM company_document
WHERE tenant_id = '86b8ad91-7463-478a-aa43-43a977105bf4' AND deleted_at >= NOW() - INTERVAL '1 minute';
COMMIT;
")

(def ^:private motivating-pine
  "What the whole script converts to."
  "company_document as d
    | company_document_attachment as cda :left .company_document_id = .id
    | attachment as a :left .id = .attachment_id
    | where: d.tenant_id = '86b8ad91-7463-478a-aa43-43a977105bf4'
    | where: d.deleted_at is null
    | group: d.id, d.company_id, d.reference_id, d.created_at, d.archived_at with id::text => min where type = 'sharepoint' as min_id, id::text => min as min_id_2, any(type = 'sharepoint') as any_type
    | where: any_type = true or reference_id is not null
    | select: id, company_id, reference_id, created_at, archived_at, coalesce(min_id, min_id_2) as any_attachment_id |= sp_docs

   sp_docs as s | select: s.*, coalesce(reference_id, concat('att:', any_attachment_id)) as dedupe_key |= grouped

   grouped as g
    | activity_resource as ar .resourceableId = .id ::text
    | activity as act .id = .activityId
    | where: tenantId = '86b8ad91-7463-478a-aa43-43a977105bf4'
    | where: userId != 'd3b93066-ea59-4259-a534-943249fba79c' or userId is null
    | where: type != 'company-document-downloaded' or type is null
    | select: g.id as document_id | from: g |= edited

   grouped as g
    | where: archived_at is null
    | select: g.*, id in edited as has_activity, if id in edited then created_at as rn_order_2
    | order: has_activity desc, rn_order_2 desc, created_at desc, id asc
    | limit: 1 per company_id, dedupe_key |= ranked

   ranked | select: id |= keep_final

   company_document as d
    | grouped as g .id = .id
    | where: archived_at is null
    | where: id not in keep_final
    | from: d
    | update! deleted_at = now()")

(deftest test-motivating-script
  (testing "The whole script converts. Its one ROW_NUMBER() that nothing uses is left out"
    (converts-to-blocks motivating-script
                        motivating-pine
                        ["Left out BEGIN and COMMIT. Pine runs each update! and delete! in a transaction of its own."
                         "Left out the statement on line 32. Pine converts one statement at a time."
                         "Left out rn_oldest_only: a ROW_NUMBER() in `ranked` that no later query uses."]
                        keyed-schema)
    (is (= (t/sql->pine motivating-sql keyed-schema)
           {:expression (:expression (t/sql->pine motivating-script keyed-schema))
            :warnings ["Left out BEGIN and COMMIT. Pine runs each update! and delete! in a transaction of its own."]})
        "The same Pine as the script without them"))

  (testing "An expression in ROW_NUMBER()'s ORDER BY is a computed column, by name"
    (let [{:keys [expression]} (t/sql->pine motivating-ranked-by-expression keyed-schema)]
      (is (s/includes? expression "| select: g.*, id in edited as rn_order_1\n | order: rn_order_1 desc, created_at desc, id asc"))))

  (testing "Pine builds every block"
    (let [sql (runs-as-pine (s/replace motivating-pine #"\n\s*\| from: d\n\s*\| update! deleted_at = now\(\)" ""))]
      (is (s/includes? sql "MIN(\"a\".\"id\"::text) FILTER (WHERE (\"a\".\"type\" = 'sharepoint'::\"public\".\"attachment_type\")) AS \"min_id\""))
      (is (s/includes? sql "ROW_NUMBER() OVER (PARTITION BY \"g\".\"company_id\", \"g\".\"dedupe_key\" ORDER BY (\"g\".\"id\" IN ( SELECT \"document_id\" FROM \"edited\" )) DESC"))))

  (testing "Without the schema, a test on the LEFT JOIN can't be NULL"
    (let [{:keys [expression]} (t/sql->pine motivating-sql)]
      (is (s/includes? expression "| select: g.*, if id in edited then true else false as has_activity, if id in edited then created_at as rn_order_2"))))

  (testing "The structure converts, close to the Pine a person would write"
    (converts-to-blocks motivating-reduced
                        "company_document as d
                          | company_document_attachment as cda :left .company_document_id = .id
                          | attachment as a :left .id = .attachment_id
                          | where: d.tenant_id = '86b8ad91-7463-478a-aa43-43a977105bf4'
                          | where: d.deleted_at is null
                          | where: type = 'sharepoint' or d.reference_id is not null
                          | select: d.id, d.company_id, d.reference_id, d.created_at, d.archived_at
                          | from: d |= sp_docs

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

;; ---------------
;; COMPUTED VALUES
;; ---------------

(deftest test-computed-values
  (testing "COALESCE, ||, CASE and NOW() in the select list"
    (converts-to "select coalesce(a, 'x' || b) as k from t" "t | select: coalesce(a, concat('x', b)) as k")
    (converts-to "select 'a' || b || c as k from t" "t | select: concat('a', b, c) as k")
    (converts-to "select case when a = 1 then 'one' when a = 2 then 'two' else 'many' end as k from t"
                 "t | select: if a = 1 then 'one' else if a = 2 then 'two' else 'many' as k")
    (converts-to "select case when a is null then b end as k from t" "t | select: if a is null then b as k")
    (converts-to "select case when a = 1 then case when b = 2 then 'x' end end as k from t"
                 "t | select: if a = 1 then (if b = 2 then 'x') as k")
    (converts-to "select now() as n from t" "t | select: now() as n")
    (converts-to "select coalesce(a::text, b) as k from t" "t | select: coalesce(a::text, b) as k"))

  (testing "A condition is a true or false value"
    (converts-to "select (a is not null) as k, (b = 1 or c like 'x%') as l from t"
                 "t | select: a is not null as k, b = 1 or c like 'x%' as l")
    (converts-to "select (a is distinct from 'x') as k from t" "t | select: a != 'x' or a is null as k"))

  (testing "With * and after a join, columns keep their alias"
    (converts-to "select s.*, coalesce(s.a, 'x') as k from s" "s as s | select: s.*, coalesce(a, 'x') as k")
    (converts-to "select coalesce(c.name, e.name) as n from company c join employee e on e.company_id = c.id"
                 "company as c | employee as e .company_id = .id | select: coalesce(c.name, name) as n"))

  (testing "NOW() in WHERE and in SET"
    (converts-to "select * from t where created_at < now()" "t | where: created_at < now()")
    (converts-to "update t set deleted_at = now() where id = 1" "t | where: id = 1 | update! deleted_at = now()"))

  (testing "ORDER BY a computed value's name"
    (converts-to "select coalesce(a, b) as k from t order by k desc" "t | select: coalesce(a, b) as k | order: k desc"))

  (testing "Refused"
    (refuses "select lower(a) as l from t" "Functions like lower() aren't in Pine.")
    (refuses "select * from t where lower(a) = 'x'" "Functions like lower() aren't in Pine here.")
    (refuses "select coalesce(a, b) from t" "Name this value with AS")
    (refuses "select concat(a, b) as k from t" "CONCAT() skips NULL arguments")
    (refuses "select (a = 1 and b = 2) as k from t" "AND inside a value isn't in Pine.")
    (converts-to "select min(a) as m from t" "t | group: with a => min as m")
    (refuses "select * from t order by coalesce(a, b)" "ORDER BY an expression isn't converted yet.")
    (refuses "select coalesce(a::int, b) as k from t" "cast to int")))

(deftest test-aggregates
  (testing "An aggregate named with AS is that column of the group:"
    (converts-to "select a, min(b) as first_b, sum(c) as total from t group by a"
                 "t | group: a with b => min as first_b, c => sum as total"))

  (testing "FILTER, BOOL_OR and BOOL_AND"
    (converts-to "select a, min(b) filter (where c = 'x') as m, bool_or(c = 'y') as some_y, bool_and(d > 1) as all_d from t group by a"
                 "t | group: a with b => min where c = 'x' as m, any(c = 'y') as some_y, all(d > 1) as all_d"))

  (testing "COUNT(column) counts the rows where the column isn't NULL"
    (converts-to "select a, count(b) as n from t group by a"
                 "t | group: a with count where b is not null as n")
    (converts-to "select u.id, count(*) as roles, count(r.archived_at) as archived from u join r on r.u_id = u.id group by u.id having count(*) = count(r.archived_at)"
                 "u as u | r .u_id = .id | group: u.id with count as roles, count where archived_at is not null as archived | where: roles = archived"))

  (testing "COUNT(column) keeps the qualifier of an earlier table"
    (converts-to "select u.id, count(u.email) as n from u join r on r.u_id = u.id group by u.id"
                 "u as u | r .u_id = .id | group: u.id with count where u.email is not null as n"))

  (testing "Without GROUP BY, COUNT(column) on its own is a where: and count:"
    (converts-to "select count(x) from t where y = 1"
                 "t | where: y = 1 | where: x is not null | count:")
    (converts-to "select count(*), count(x) from t" "t | group: with count, count where x is not null as count_2"
                 ["`count_2` is the SQL's column `count`: Pine needs each name once."]))

  (testing "COUNT(column) with FILTER needs AND, which a count's filter doesn't have"
    (refuses "select a, count(b) filter (where c = 1) as n from t group by a" "COUNT(column) with FILTER isn't in Pine"))

  (testing "HAVING is a where: after the group:, on the names"
    (converts-to "select a, count(*) from t group by a having count(*) > 1"
                 "t | group: a with count | where: count > 1")
    (converts-to "select a, max(b) as m from t group by a having max(b) > 10 or a is null"
                 "t | group: a with b => max as m | where: m > 10 or a is null"))

  (testing "An aggregate only HAVING uses gets a name, and a select: leaves it out"
    (converts-to "select a from t group by a having bool_or(b = 1)"
                 "t | group: a with any(b = 1) as any_b | where: any_b = true | select: a"))

  (testing "A value over aggregates is computed after the group:"
    (converts-to "select a, coalesce(min(b) filter (where c = 1), min(b)) as m from t group by a"
                 "t | group: a with b => min where c = 1 as min_b, b => min as min_b_2 | select: a, coalesce(min_b, min_b_2) as m"))

  (testing "ORDER BY and LIMIT after them, by name"
    (converts-to "select a, coalesce(min(b), 0) as m from t group by a order by m desc limit 5"
                 "t | group: a with b => min as min_b | select: a, coalesce(min_b, 0) as m | order: m desc | limit: 5"))

  (testing "Inside a WITH query the columns are exactly the SQL's"
    (converts-to-blocks "with g as (select a, sum(b) as total from t group by a having sum(b) > 1) select * from g where total < 100"
                        "t | group: a with b => sum as total | where: total > 1 |= g

                         g | where: total < 100"))

  (testing "An aggregate without AS is named as Postgres names it: after its function"
    (converts-to "select a, sum(b) from t group by a" "t | group: a with b => sum as sum"))

  (testing "Refused"
    (refuses "select a, b, min(c) as m from t group by a" "`b` is selected but not grouped.")
    (refuses "select a from t group by a having b > 1" "`b` is neither grouped nor inside an aggregate.")
    (refuses "select a, min(max(b)) as m from t group by a" "An aggregate inside another isn't in Pine.")
    (refuses "select count(*) from t having count(*) > 1" "HAVING without GROUP BY")))

(deftest test-window-expressions
  (testing "An expression in PARTITION BY or ORDER BY is selected as a computed column, and ranked by name"
    (converts-to-blocks "with r as (select t.*, row_number() over (partition by coalesce(t.a, 'x') order by (t.b is null) desc, t.c) as rn from t)
                         select id from r where rn = 1"
                        "t as t | select: t.*, b is null as rn_order_1, coalesce(a, 'x') as rn_partition_1
                           | order: rn_order_1 desc, c asc | limit: 1 per rn_partition_1 |= r

                         r | select: id"))

  (testing "A selected value with the same expression is ranked by its name"
    (converts-to-blocks "with r as (select t.*, (t.b is null) as no_b, row_number() over (partition by t.a order by (t.b is null) desc) as rn from t)
                         select id from r where rn = 1"
                        "t as t | select: t.*, b is null as no_b | order: no_b desc | limit: 1 per a |= r

                         r | select: id"))

  (testing "The added column is said when it reaches the result"
    (is (= ["`rn` from ROW_NUMBER() isn't a column in Pine: `r` keeps the first rows of each group instead."
            "`rn_order_1` is a column the SQL doesn't have. Pine ranks rows by named columns, so an expression in ROW_NUMBER()'s ORDER BY is selected as one."]
           (:warnings (t/sql->pine "with r as (select t.*, row_number() over (partition by t.a order by (t.b is null) desc) as rn from t) select * from r where rn = 1")))))

  (testing "A ROW_NUMBER() no later query uses is left out; one a * returns is not"
    (converts-to-blocks "with r as (select t.*, row_number() over (partition by t.a order by t.c) as rn, row_number() over (order by t.d) as unused from t)
                         select id from r where rn = 1"
                        "t | order: c asc | limit: 1 per a |= r

                         r | select: id"
                        ["Left out unused: a ROW_NUMBER() in `r` that no later query uses."])
    (refuses "with r as (select id, row_number() over (partition by a order by b) as rn from t) select * from r"
             "must keep its first rows")))

(deftest test-semi-joins
  (testing "A LEFT JOIN to distinct values, only tested for NULL, is `in`"
    (converts-to-blocks "with k as (select distinct e.company_id from employee e)
                         select c.id, (k.company_id is not null) as has_staff from company c left join k on k.company_id = c.id"
                        "employee as e | select: company_id |= k

                         company as c | select: id, if id in k then true else false as has_staff"))

  (testing "With the schema saying neither side is NULL, it is plain `in`"
    (converts-to-blocks "with k as (select distinct d.id from company_document d where d.reference_id = 'x')
                         select g.id, case when k.id is not null then 'dup' end as tag, (k.id is null) as fresh from company_document g left join k on k.id = g.id"
                        "company_document as d | where: reference_id = 'x' | select: id |= k

                         company_document as g | select: id, if id in k then 'dup' as tag, id not in k as fresh"
                        [] keyed-schema))

  (testing "Mentioned anywhere else, as in ORDER BY, it stays a join, whose DISTINCT Pine can't say"
    (refuses "with k as (select distinct e.company_id from employee e)
              select c.id, (k.company_id is not null) as has_staff from company c left join k on k.company_id = c.id order by k.company_id"
             "DISTINCT isn't in Pine"))

  (testing "Without DISTINCT the join can repeat rows, so it stays a join"
    (converts-to-blocks "with k as (select e.company_id from employee e)
                         select c.id, (k.company_id is not null) as has_staff from company c left join k on k.company_id = c.id"
                        "employee as e | select: company_id |= k

                         company as c | k :left .company_id = .id | select: c.id, company_id is not null as has_staff")))

(deftest test-exists
  (testing "EXISTS is `in`: the subquery selects the column it is tied by"
    (converts-to-blocks "select * from t where exists (select 1 from k where k.t_id = t.id and k.x = 1)"
                        "k | where: x = 1 | select: t_id |= k_t_ids

t | where: id in k_t_ids"))

  (testing "Either side of the equality may be the outer column, and the subquery may join"
    (converts-to-blocks "select * from t where exists (select * from k join m on m.k_id = k.id where t.id = k.t_id and m.z = 2 limit 1)"
                        "k as k | m .k_id = .id | where: z = 2 | select: k.t_id | from: k |= k_t_ids

t | where: id in k_t_ids"))

  (testing "NOT EXISTS is `not in`, guarded against NULLs without the schema"
    (converts-to-blocks "select * from t where not exists (select 1 from k where k.t_id = t.id)"
                        "k | where: t_id is not null | select: t_id |= k_t_ids

t | where: id is null or id not in k_t_ids"))

  (testing "Under OR, NOT EXISTS stays one where: step"
    (converts-to-blocks "select * from t where a = 1 or not exists (select 1 from k where k.t_id = t.id)"
                        "k | where: t_id is not null | select: t_id |= k_t_ids

t | where: a = 1 or id is null or id not in k_t_ids"))

  (testing "NOT EXISTS picks the rows an UPDATE changes"
    (converts-to-blocks "update t set a = 1 where not exists (select 1 from k where k.t_id = t.id)"
                        "k | where: t_id is not null | select: t_id |= k_t_ids

t | where: id is null or id not in k_t_ids | update! a = 1"))

  (testing "NOT EXISTS over a WITH query keeps the guard: the WITH query isn't narrowed"
    (converts-to-blocks "with k as (select t_id from m) select * from t where not exists (select 1 from k where k.t_id = t.id)"
                        "m | select: t_id |= k

k | where: t_id is not null | select: t_id |= k_t_ids

t | where: id is null or id not in k_t_ids"))

  (testing "An EXISTS inside another is refused"
    (refuses "select * from t where exists (select 1 from k where k.t_id = t.id and exists (select 1 from m where m.k_id = k.id))"
             "An EXISTS inside another EXISTS isn't converted."))

  (testing "The subquery must be tied to the outer query by one equality, and only by it"
    (refuses "select * from t where exists (select 1 from k where k.x = 1)" "doesn't refer to the outer query")
    (refuses "select * from t where exists (select 1 from k where k.t_id = t.id or k.x = 1)" "tied to the outer query by one equality")
    (refuses "select * from t where exists (select 1 from k where k.t_id = t.id and k.x = t.y)" "tied to the outer query by one equality")
    (refuses "select * from t where exists (select 1 from k where k.t_id > t.id)" "tied to the outer query by one equality")
    (refuses "select * from t where exists (select 1 from k join m on m.t_id = t.id where k.t_id = t.id)" "also refers to it elsewhere"))

  (testing "An alias the subquery reuses is the subquery's own"
    (refuses "select * from t u where exists (select 1 from k u where u.x = 1)" "doesn't refer to the outer query"))

  (testing "Elsewhere than WHERE, EXISTS is refused"
    (refuses "select (exists (select 1 from k where k.t_id = t.id)) as e from t" "EXISTS converts only in WHERE")
    (refuses "select * from t join k on k.t_id = t.id and exists (select 1 from m where m.k_id = k.id)" "EXISTS converts only in WHERE")
    (refuses "select * from t where exists (select k.t_id from k where k.t_id = t.id group by k.t_id)" "GROUP BY")))

(deftest test-positions
  (testing "ORDER BY a position sorts by that selected column, under its name"
    (converts-to "select a, b from t order by 2, 1 desc" "t | select: a, b | order: b asc, a desc")
    (converts-to "select a, coalesce(b, 'x') as k from t order by 2" "t | select: a, coalesce(b, 'x') as k | order: k asc"))

  (testing "GROUP BY a position groups by that selected column"
    (converts-to "select a, count(*) from t group by 1 order by 2 desc" "t | group: a with count | order: count desc")
    (converts-to-blocks "with s as (select a, b, count(*) as n from t group by 1, 2) select a, count(*) as subjects, max(n) as max_n from s group by 1 order by 3 desc limit 20"
                        "t | group: a, b with count as n |= s

s | group: a with count as subjects, n => max as max_n | order: max_n desc | limit: 20"))

  (testing "A position that numbers no column, a value, or past a `*` is refused"
    (refuses "select a from t order by 3" "ORDER BY 3: the SELECT has only 1 column.")
    (refuses "select coalesce(a, b) as k, count(*) from t group by 1" "GROUP BY 1 numbers a value, not a column.")
    (refuses "select *, a from t group by 2" "GROUP BY 2 counts past a `*`")))

(deftest test-select-of-an-earlier-table
  (testing "A select list of only an earlier table's columns ends on that table, or Pine would show the last table's columns too"
    (converts-to "select sc.a, sc.b from s sc join o on o.id = sc.o_id where o.n = 'x'"
                 "s as sc | o .id = .o_id | where: n = 'x' | select: sc.a, sc.b | from: sc")
    (converts-to "select sc.* from s sc join o on o.id = sc.o_id"
                 "s as sc | o .id = .o_id | select: sc.* | from: sc"))
  (testing "A select list with a column of the last table needs no from:"
    (converts-to "select sc.a, o.n from s sc join o on o.id = sc.o_id"
                 "s as sc | o .id = .o_id | select: sc.a, n")))

(deftest test-aggregates-without-group-by
  (testing "COUNT(*) with a name of its own is one group, so the name is kept"
    (converts-to "SELECT COUNT(*) AS active_customers FROM customers WHERE is_active"
                 "customers | where: is_active = true | group: with count as active_customers"))
  (testing "A plain COUNT(*) stays count:"
    (converts-to "select count(*) from t" "t | count:")
    (converts-to "select count(*) as count from t" "t | count:"))
  (testing "Several aggregates without GROUP BY are totals over the whole table"
    (converts-to "SELECT COUNT(*) AS orders, SUM(total) AS revenue FROM orders WHERE status = 'shipped'"
                 "orders | where: status = 'shipped' | group: with count as orders, total => sum as revenue")
    (converts-to "select sum(total) from orders" "orders | group: with total => sum as sum"))
  (testing "COUNT(*) FILTER converts once there is a group to count in"
    (converts-to "select count(*) filter (where status = 'x') as n from t"
                 "t | group: with count where status = 'x' as n"))
  (testing "A column next to an aggregate still needs GROUP BY"
    (refuses "select count(*) as n, name from t" "A column next to an aggregate needs GROUP BY.")))

(deftest test-json-array-elements
  (testing "jsonb_array_elements in FROM is Pine's `[*]`: one row for each element"
    (converts-to "select a.id, e->>'k' as k from a cross join lateral jsonb_array_elements(a.data) e"
                 "a as a | a.data[*] as e | select: a.id, e.k as k")
    (converts-to "select a.id, e->>'k' as k from a, lateral json_array_elements(a.data->'items') as e"
                 "a as a | a.data.items[*] as e | select: a.id, e.k as k")
    (converts-to "select a.id, e->>'k' as k from a join jsonb_array_elements(a.data) e on true"
                 "a as a | a.data[*] as e | select: a.id, e.k as k"))

  (testing "The element itself is its alias. SQL names it `value`, or what e(x) says"
    (converts-to "select a.id, e from a, jsonb_array_elements_text(a.data->'tags') e"
                 "a as a | a.data.tags[*] as e | select: a.id, e")
    (converts-to "select a.id, e.value from a, jsonb_array_elements_text(a.data) e"
                 "a as a | a.data[*] as e | select: a.id, e as value")
    (converts-to "select a.id, e.tag from a, jsonb_array_elements_text(a.data) e(tag)"
                 "a as a | a.data[*] as e | select: a.id, e as tag")
    (converts-to "select * from a cross join lateral jsonb_array_elements_text(a.data) e"
                 "a as a | a.data[*] as e | select: a.*, e as value"))

  (testing "jsonb_array_elements gives JSON, so a string keeps its quotes in SQL"
    (converts-to "select a.id, e from a, jsonb_array_elements(a.data) e"
                 "a as a | a.data[*] as e | select: a.id, e"
                 ["In SQL, jsonb_array_elements() gives JSON, so a string keeps its quotes. Pine shows e as text, without them, as jsonb_array_elements_text() does."]))

  (testing "Elements of elements"
    (converts-to "select i->>'sku' as sku from a cross join lateral jsonb_array_elements(a.data->'orders') o cross join lateral jsonb_array_elements(o->'items') i"
                 "a as a | a.data.orders[*] as o | o.items[*] as i | select: i.sku as sku"))

  (testing "A table joined after the elements is joined to its own table"
    (converts-to "select a.id, b.name from a cross join lateral jsonb_array_elements(a.data) e join b on b.id = a.b_id where e->>'k' = 'v'"
                 "a as a | a.data[*] as e | from: a | b .id = .b_id | where: e.k = 'v' | select: a.id, name"))

  (testing "An element of jsonb_array_elements_text compares as text"
    (converts-to "select a.id from a, jsonb_array_elements_text(a.tags) e where e != 'red'"
                 "a as a | a.tags[*] as e | where: e != 'red' | where: e is not null | select: a.id | from: a")
    (converts-to "select a.id from a, jsonb_array_elements_text(a.tags) e where e = 'red'"
                 "a as a | a.tags[*] as e | where: e = 'red' | select: a.id | from: a")
    (refuses "select a.id from a, jsonb_array_elements_text(a.tags) e where e = '5'" "'5' is also how a JSON number reads as text")
    (refuses "select a.id from a, jsonb_array_elements(a.tags) e where e = '\"red\"'" "Use jsonb_array_elements_text()"))

  (testing "ROW_NUMBER() over a key of the elements ranks by the key"
    (converts-to-blocks "select * from (select a.id, e->>'k' as k, row_number() over (partition by a.id, e->>'k' order by e->'n' desc) as rn from a cross join lateral jsonb_array_elements(a.data) e) x where rn = 1"
                        "a as a | a.data[*] as e | select: a.id, e.k as k | order: e.n desc | limit: 1 per a.id, e.k |= x\n\nx"
                        ["`rn` from ROW_NUMBER() isn't a column in Pine: `x` keeps the first rows of each group instead."])
    (refuses "select * from (select a.id, row_number() over (partition by e->'k') as rn from a, jsonb_array_elements(a.data) e) x where rn = 1"
             "PARTITION BY a -> key isn't converted"))

  (testing "Refused"
    (refuses "select a.id from a left join lateral jsonb_array_elements(a.data) e on true" "A LEFT JOIN LATERAL keeps the rows whose array is empty")
    (refuses "select a.id from a cross join lateral jsonb_array_elements(a.data->>'x') e" "->> gives text. Use ->")
    (refuses "select * from a cross join b" "A CROSS JOIN converts only for the elements of a JSON array")
    (refuses "select * from a cross join lateral generate_series(1, 3) g" "A CROSS JOIN converts only for the elements of a JSON array")))

(deftest test-reported-edits-query
  (testing "The last edit of each field, from the elements of a JSON array"
    (converts-to-blocks
     "WITH edits AS (
  SELECT ar.\"resourceableId\"::uuid AS company_id, a.\"createdAt\", a.updated_by_email,
         e->>'field' AS field, e->'after' AS manual_value,
         row_number() OVER (PARTITION BY ar.\"resourceableId\", e->>'field' ORDER BY a.\"createdAt\" DESC) AS rn
  FROM activity a
  JOIN activity_resource ar ON ar.\"activityId\" = a.id AND ar.\"resourceableType\" = 'Company'
  CROSS JOIN LATERAL jsonb_array_elements(a.data) e
  WHERE a.\"tenantId\" = '1d4ec3f6-2046-4b18-aea2-c27c80e89103' AND a.type = 'company-edited'
    AND a.\"createdAt\" >= '2026-01-01 00:00' AND e->>'field' IN ('legalTypes', 'giin'))
SELECT c.name, ed.field, ed.manual_value, ed.\"createdAt\", ed.updated_by_email,
       c.legal_types AS current_legal_types, c.giin AS current_giin
FROM edits ed JOIN company c ON c.id = ed.company_id
WHERE ed.rn = 1
ORDER BY c.name, ed.field;"
     "activity as a
 | activity_resource as ar .activityId = .id
 | a.data[*] as e
 | where: ar.resourceableType = 'Company'
 | where: a.tenantId = '1d4ec3f6-2046-4b18-aea2-c27c80e89103'
 | where: a.type = 'company-edited'
 | where: a.createdAt >= '2026-01-01 00:00'
 | where: e.field in ('legalTypes', 'giin')
 | select: ar.resourceableId::uuid as company_id, a.createdAt, a.updated_by_email, e.field as field, e.after as manual_value
 | order: a.createdAt desc
 | limit: 1 per ar.resourceableId, e.field |= edits

edits as ed
 | company as c .id = .company_id
 | select: name, ed.field, ed.manual_value, ed.createdAt, ed.updated_by_email, legal_types as current_legal_types, giin as current_giin
 | order: name asc, ed.field asc"
     ["A condition in an inner join's ON moved to where:. It means the same there."
      "In SQL, `->` gives JSON, so a string keeps its quotes. Pine shows e.after as text, without them, as `->>` does."])))
