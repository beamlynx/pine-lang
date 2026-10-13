(ns pine.sql.parser-test
  (:require [clojure.string :as s]
            [clojure.test :refer [deftest is testing]]
            [pine.sql.parser :as sp]))

(defn- p [sql] (:result (sp/parse sql)))

(defn- strip-at
  "The parse without :at, so a test compares shape only."
  [x]
  (cond
    (map? x) (into {} (for [[k v] (dissoc x :at)] [k (strip-at v)]))
    (vector? x) (mapv strip-at x)
    :else x))

(def corpus
  "Statements the grammar must read exactly one way."
  ["select * from company"
   "SELECT * FROM company;"
   "select name, id from company"
   "select name as n, id i from company"
   "select c.* from company c join employee e on e.company_id = c.id"
   "select * from public.company as c left join employee e on e.company_id = c.id"
   "select * from a right outer join b on b.a_id = a.id"
   "select * from a inner join b on b.a_id = a.id and b.x = a.y"
   "select * from a join b using (a_id, x)"
   "select * from t where a = 1 and b = 'x' or c = true"
   "select * from t where (a = 1 or b = 2) and not (c = 3 and d = 4)"
   "select * from t where a <> 1 and b != 2 and c >= 3 and d <= 4 and e > 5 and f < 6"
   "select * from t where a like 'x%' and b not ilike '%y' and c is null and d is not null"
   "select * from t where a in ('x', 'y') and b not in (1, 2.5, -3)"
   "select * from t where a between 1 and 5 and b not between 'a' and 'b'"
   "select * from t where active and not deleted"
   "select * from t where x::text = '1' and cast(y as uuid) = 'a' and z > '2024-01-01'::timestamp"
   "select * from t where t.created_at > '2024-01-01 10:00'::timestamp with time zone"
   "select * from t where a = $name"
   "select count(*) from t where a = 1"
   "select status, count(*) as n from t group by status order by count(*) desc"
   "select a, b from t group by a, b order by a, b desc limit 10"
   "select * from t order by 1"
   "select * from t where color = 1 or order_id = 2"
   "select \"c_0\".\"id\" as \"__c_0__id\", \"e_1\".* from \"company\" as \"c_0\" join \"employee\" as \"e_1\" on \"c_0\".\"id\" = \"e_1\".\"company_id\" limit 250"
   "select `id` from `Order` -- MySQL quoting\n"
   "/* a comment */ select * -- and another\n from t"
   "with a as (select id from t), b as (select * from a where id > 1) select * from b"
   "select distinct a from t"
   "select distinct on (a, b) a, b, c from t order by a, b, c desc"
   "select t.*, row_number() over (partition by t.a, t.b order by t.c desc, t.d) as rn from t"
   "select row_number() over (order by c) rn from t"
   "select * from (select id from t) x join (select id from u) as y on y.id = x.id"
   "select * from t where a in (select id from u where b = 1) and c not in (select id from v)"
   "select * from t where a is distinct from 'x' and b is not distinct from 1"
   "update t set a = 1, b = null where id = 2"
   "update t as d set a = 'x' from g left join k on k.id = g.id where d.id = g.id and k.id is null"
   "delete from t where id = 1"
   "delete from t d using g where d.id = g.id"
   "with k as (select id from t) delete from u where id in (select id from k)"
   "update company_document d set deleted_at = '2026-10-12' from sp_docs g left join keep_final kf on kf.id = g.id where d.id = g.id and kf.id is null and g.archived_at is null"
   ;; Computed values.
   "select coalesce(a, 'x' || b) as k, (c is not null) as d, case when e = 1 then 'one' when e = 2 then 'two' else 'many' end as f from t"
   "select coalesce(a, b)::text as k, now() as n from t where c > now()"
   "select a, min(b::text) filter (where c = 'x') as m, bool_or(c = 'y') as o, count(*) from t group by a having bool_or(c = 'y') or a is not null"
   "select a, count(*) from t group by a having count(*) > 1 and min(b) < 5"
   "select t.*, row_number() over (partition by t.a, coalesce(t.b, 'x') order by (t.c is null) desc, case when t.d then t.e end desc) as rn from t"
   "select * from t order by coalesce(a, b) desc"
   "update t set a = now() where id = 1"
   "select * from a join b on b.x = a.y::text"
   "select lower(a) from t where upper(b) = 'X'"
   "select distinct on (a) a, b from t order by a, b desc"
   ;; The structure of the script in transpile_test's test-motivating-script.
   "WITH sp_docs AS (\n    SELECT d.id, d.company_id, d.reference_id, d.created_at, d.archived_at\n    FROM company_document d\n    LEFT JOIN company_document_attachment cda ON cda.company_document_id = d.id\n    LEFT JOIN attachment a ON a.id = cda.attachment_id\n    WHERE d.tenant_id = '86b8ad91-7463-478a-aa43-43a977105bf4' AND d.deleted_at IS NULL\n      AND (a.type = 'sharepoint' OR d.reference_id IS NOT NULL)\n),\nranked AS (\n    SELECT g.*,\n        ROW_NUMBER() OVER (PARTITION BY g.company_id, g.reference_id ORDER BY g.created_at DESC, g.id ASC) AS rn\n    FROM sp_docs g WHERE g.archived_at IS NULL\n),\nkeep_final AS (SELECT id FROM ranked WHERE rn = 1)\nUPDATE company_document d SET deleted_at = '2026-10-12'\nFROM sp_docs g LEFT JOIN keep_final kf ON kf.id = g.id\nWHERE d.id = g.id AND kf.id IS NULL AND g.archived_at IS NULL"])

(deftest test-one-parse
  (doseq [sql corpus]
    (testing sql
      (is (= 1 (sp/parse-count sql))))))

(deftest test-shape
  (testing "Select list, table, alias"
    (is (= {:kind :select :ctes [] :joins []
            :select [{:kind :column :column {:name "name"}} {:kind :column :column {:name "id" :qualifier "c"} :as "i"}
                     {:kind :count :as "n"} {:kind :alias-star :qualifier "c"} {:kind :star}]
            :from {:schema "public" :table "company" :alias "c"}}
           (strip-at (p "SELECT name, c.id AS i, COUNT(*) n, c.*, * FROM public.company c")))))

  (testing "Joins"
    (is (= [{:type :left :table {:table "employee" :alias "e"}
             :on {:op :compare :operator "="
                  :left {:kind :column :column {:name "company_id" :qualifier "e"}}
                  :right {:kind :column :column {:name "id" :qualifier "c"}}}}
            {:type nil :table {:table "invoice"} :using [{:name "company_id"}]}
            {:type :right :table {:table "x"} :using [{:name "a"} {:name "B" :quoted true}]}]
           (:joins (strip-at (p "select * from company c left outer join employee e on e.company_id = c.id join invoice using (company_id) right join x using (a, \"B\")"))))))

  (testing "Connectives: a run of one connective is one node"
    (is (= {:op :or
            :args [{:op :and :args [{:op :truthy :column {:name "a"}}
                                    {:op :truthy :column {:name "b"}}
                                    {:op :truthy :column {:name "c"}}]}
                   {:op :not :arg {:op :truthy :column {:name "d"}}}]}
           (:where (strip-at (p "select * from t where a and b and c or not d"))))))

  (testing "Comparisons"
    (let [w (fn [cond] (:where (strip-at (p (str "select * from t where " cond)))))]
      (is (= {:op :compare :operator "!=" :left {:kind :column :column {:name "a"}} :right {:kind :number :value "1"}}
             (w "a <> 1")))
      (is (= {:op :like :negated true :operator "ilike" :left {:kind :column :column {:name "a"}} :right {:kind :string :value "O'B%"}}
             (w "a NOT ILIKE 'O''B%'")))
      (is (= {:op :is-null :negated true :left {:kind :column :column {:name "a"}}}
             (w "a is not null")))
      (is (= {:op :in :negated false :left {:kind :column :column {:name "a"}}
              :values [{:kind :number :value "-1.5"} {:kind :string :value "x"}]}
             (w "a in (-1.5, 'x')")))
      (is (= {:op :between :negated false :left {:kind :column :column {:name "a"}}
              :low {:kind :number :value "1"} :high {:kind :number :value "2"}}
             (w "a between 1 and 2")))
      (is (= {:op :compare :operator "=" :left {:kind :column :column {:name "a"} :cast "text"} :right {:kind :string :value "1"}}
             (w "CAST(a AS TEXT) = '1'")))
      (is (= {:op :compare :operator ">" :left {:kind :column :column {:name "a"}}
              :right {:kind :string :value "2024-01-01" :cast "timestamp with time zone"}}
             (w "a > '2024-01-01'::TIMESTAMP WITH TIME ZONE")))
      (is (= {:op :compare :operator "=" :left {:kind :column :column {:name "a"}} :right {:kind :boolean :value false}}
             (w "a = FALSE")))
      (is (= {:op :compare :operator "=" :left {:kind :column :column {:name "a"}} :right {:kind :variable :name "Name"}}
             (w "a = $Name")))))

  (testing "Group, order, limit"
    (is (= {:group [{:name "status"}]
            :order [{:target {:kind :count} :direction :desc}
                    {:target {:kind :column :column {:name "status"}} :direction nil}
                    {:target {:kind :position :value "1"} :direction :asc}]
            :limit {:value 5}}
           (select-keys (strip-at (p "select status, count(*) from t group by status order by count(*) DESC, status, 1 ASC limit 5"))
                        [:group :order :limit])))))

(deftest test-case
  (testing "Keywords in any case"
    (is (= (strip-at (p "select * from t where a = 1 order by a desc"))
           (strip-at (p "SeLeCt * FROM t WhErE a = 1 ORDER BY a DESC")))))

  (testing "A bare identifier is folded to lowercase, as Postgres reads it. A quoted one keeps its case, and is marked"
    (is (= {:table "users" :alias "u"} (dissoc (:from (p "select * from Users U")) :at)))
    (is (= {:table "Users" :quoted true} (dissoc (:from (p "select * from \"Users\"")) :at)))
    (is (= {:table "Order" :quoted true} (dissoc (:from (p "select * from `Order`")) :at)))
    (is (= {:table "a\"b" :quoted true} (dissoc (:from (p "select * from \"a\"\"b\"")) :at)))
    (is (= {:schema "App" :schema-quoted true :table "users"} (dissoc (:from (p "select * from \"App\".Users")) :at)))
    (is (= {:name "email" :qualifier "u"} (dissoc (-> (p "select U.EMAIL from users u") :select first :column) :at)))
    (is (= {:name "tenantId" :qualifier "u" :quoted true} (dissoc (-> (p "select U.\"tenantId\" from users u") :select first :column) :at)))))

(deftest test-positions
  (testing ":at is the offset a refusal points at"
    (let [sql "select *\nfrom t\nwhere a = 1"
          at (-> (p sql) :where :at)]
      (is (= "a = 1" (subs sql at)))
      (is (= 3 (sp/line-of sql at))))))

(deftest test-new-shapes
  (testing "WITH queries, in order"
    (is (= ["a" "b"] (map :name (:ctes (p "with a as (select * from t), b as (select * from a) select * from b"))))))
  (testing "A subquery in FROM keeps its alias"
    (is (= "x" (:alias (:from (p "select * from (select id from t) x"))))))
  (testing "ROW_NUMBER()"
    (is (= {:kind :row-number :as "rn" :partition [{:name "a"}]
            :order [{:target {:kind :column :column {:name "b"}} :direction :desc}]}
           (strip-at (second (:select (p "select *, row_number() over (partition by a order by b desc) as rn from t")))))))
  (testing "UPDATE and DELETE"
    (is (= {:kind :update :target {:table "t" :alias "d"} :ctes [] :joins []
            :set [{:column {:name "a"} :value {:kind :number :value "1"}}]
            :from {:table "g"}
            :where {:op :compare :operator "=" :left {:kind :column :column {:name "id" :qualifier "d"}}
                    :right {:kind :column :column {:name "id" :qualifier "g"}}}}
           (strip-at (p "update t d set a = 1 from g where d.id = g.id"))))
    (is (= :delete (:kind (p "delete from t where id = 1"))))))

(deftest test-scripts
  (let [sql "BEGIN;\nselect 'a;b' from t; -- a comment;\nCOMMIT;\n"
        pieces (map (fn [{:keys [start end]}] (subs sql start end)) (sp/split-script sql))]
    (testing "A script splits on the `;` that end statements, not on one in a string or a comment"
      (is (= ["BEGIN" "\nselect 'a;b' from t" " -- a comment;\nCOMMIT"] pieces)))
    (testing "BEGIN, COMMIT and ROLLBACK are told apart from the rest"
      (is (= ["begin" nil "commit"] (map sp/transaction-word pieces)))
      (is (= "rollback" (sp/transaction-word "ROLLBACK WORK")))
      (is (= "begin" (sp/transaction-word "start transaction"))))
    (testing "Whether a statement changes data, even when it doesn't parse"
      (is (= :update (sp/statement-kind "with a as (select f(x) from t) update t set a = 1")))
      (is (= :delete (sp/statement-kind "DELETE FROM t")))
      (is (= :other-write (sp/statement-kind "insert into t values (1)")))
      (is (= :select (sp/statement-kind "select count(*) from t"))))
    (testing "Masking keeps offsets and lines"
      (is (= "      \nselect" (sp/mask "BEGIN;\nselect" 6 13))))
    (testing "A `;` inside a dollar-quoted body doesn't end the statement"
      (is (= 2 (count (sp/split-script "DO $$ BEGIN UPDATE t SET a = 1; END $$; select 1 from t")))))
    (testing "Statements that may change data never count as reads"
      (is (= :other-write (sp/statement-kind "CALL refresh()")))
      (is (= :other-write (sp/statement-kind "DO $$ BEGIN END $$")))
      (is (= :other-write (sp/statement-kind "EXPLAIN ANALYZE UPDATE t SET a = 1"))))))

(deftest test-errors
  (let [err (fn [sql] (select-keys (sp/parse sql) [:error :error-type]))
        unsupported (fn [sql message]
                      (let [{:keys [error error-type]} (err sql)]
                        (is (= "sql-unsupported" error-type) sql)
                        (is (some-> error (s/includes? message)) (str sql " => " error))))]
    (testing "Each names the construct"
      (unsupported "select * from t offset 5" "OFFSET")
      (unsupported "select * from t limit 5 offset 5" "OFFSET")
      (unsupported "select * from t fetch first 5 rows only" "FETCH")
      (unsupported "select * from a union select * from b" "UNION")
      (unsupported "select * from a full join b on a.id = b.a_id" "FULL JOIN")
      (unsupported "select * from a cross join b" "CROSS JOIN")
      (unsupported "select * from a natural join b" "NATURAL JOIN")
      (unsupported "select * from a, b" "comma between tables")
      (unsupported "with recursive x as (select * from t) select * from x" "WITH RECURSIVE")
      (unsupported "with x as materialized (select * from t) select * from x" "MATERIALIZED")
      (unsupported "update t set a = 1 where id = 2 returning *" "RETURNING")
      (unsupported "select row_number() over (partition by a order by b nulls last) from t" "ORDER BY b nulls last inside ROW_NUMBER()")
      (unsupported "select * from t where exists (select 1)" "EXISTS")
      (unsupported "select * from t order by a nulls last" "NULLS")
      (unsupported "select * from t where a = (select id from b)" "subquery here")
      (unsupported "select count(id) from t" "COUNT(*)")
      (unsupported "select count(distinct id) from t" "COUNT(*)")
      (unsupported "select a + 1 from t" "Arithmetic")
      (unsupported "select * from t where a = $1" "$1")
      (unsupported "select * from t where a = ?" "placeholder")
      (unsupported "select * from t; select * from u" "one statement")
      (unsupported "insert into t values (1)" "INSERT doesn't convert to Pine. Only SELECT, UPDATE and DELETE do."))

    (testing "Text that isn't SQL"
      (is (= {:error "Line 1: A string is missing its closing quote." :error-type "sql-parse-error"}
             (err "select * from t where a = 'x")))
      (is (= {:error "Line 1: The SQL ends before the statement does." :error-type "sql-parse-error"}
             (err "select * from")))
      (is (= "sql-parse-error" (:error-type (err "company | where: id = 1")))))

    (testing "The line is where the reader stopped"
      (is (= "Line 3: OFFSET isn't in Pine." (:error (err "select *\nfrom t\noffset 5")))))))
