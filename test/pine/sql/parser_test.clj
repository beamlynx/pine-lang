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
   "/* a comment */ select * -- and another\n from t"])

(deftest test-one-parse
  (doseq [sql corpus]
    (testing sql
      (is (= 1 (sp/parse-count sql))))))

(deftest test-shape
  (testing "Select list, table, alias"
    (is (= {:joins []
            :select [{:kind :column :column {:name "name"}} {:kind :column :column {:name "id" :qualifier "c"} :as "i"}
                     {:kind :count :as "n"} {:kind :alias-star :qualifier "c"} {:kind :star}]
            :from {:schema "public" :table "company" :alias "c"}}
           (strip-at (p "SELECT name, c.id AS i, COUNT(*) n, c.*, * FROM public.company c")))))

  (testing "Joins"
    (is (= [{:type :left :table {:table "employee" :alias "e"}
             :on {:op :compare :operator "="
                  :left {:kind :column :column {:name "company_id" :qualifier "e"}}
                  :right {:kind :column :column {:name "id" :qualifier "c"}}}}
            {:type nil :table {:table "invoice"} :using ["company_id"]}
            {:type :right :table {:table "x"} :using ["a" "b"]}]
           (:joins (strip-at (p "select * from company c left outer join employee e on e.company_id = c.id join invoice using (company_id) right join x using (a, b)"))))))

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

  (testing "A bare identifier is folded to lowercase, as Postgres reads it. A quoted one keeps its case"
    (is (= {:table "users" :alias "u"} (dissoc (:from (p "select * from Users U")) :at)))
    (is (= {:table "Users"} (dissoc (:from (p "select * from \"Users\"")) :at)))
    (is (= {:table "Order"} (dissoc (:from (p "select * from `Order`")) :at)))
    (is (= {:table "a\"b"} (dissoc (:from (p "select * from \"a\"\"b\"")) :at)))
    (is (= {:name "email" :qualifier "u"} (dissoc (-> (p "select U.EMAIL from users u") :select first :column) :at)))))

(deftest test-positions
  (testing ":at is the offset a refusal points at"
    (let [sql "select *\nfrom t\nwhere a = 1"
          at (-> (p sql) :where :at)]
      (is (= "a = 1" (subs sql at)))
      (is (= 3 (sp/line-of sql at))))))

(deftest test-errors
  (let [err (fn [sql] (select-keys (sp/parse sql) [:error :error-type]))
        unsupported (fn [sql message]
                      (let [{:keys [error error-type]} (err sql)]
                        (is (= "sql-unsupported" error-type) sql)
                        (is (some-> error (s/includes? message)) (str sql " => " error))))]
    (testing "Each names the construct"
      (unsupported "select distinct a from t" "DISTINCT")
      (unsupported "select * from t offset 5" "OFFSET")
      (unsupported "select * from t limit 5 offset 5" "OFFSET")
      (unsupported "select * from t fetch first 5 rows only" "FETCH")
      (unsupported "select a, count(*) from t group by a having count(*) > 1" "HAVING")
      (unsupported "select * from a union select * from b" "UNION")
      (unsupported "select * from a full join b on a.id = b.a_id" "FULL JOIN")
      (unsupported "select * from a cross join b" "CROSS JOIN")
      (unsupported "select * from a natural join b" "NATURAL JOIN")
      (unsupported "select * from a, b" "comma between tables")
      (unsupported "with x as (select 1) select * from x" "WITH")
      (unsupported "select case when a then 1 end from t" "CASE")
      (unsupported "select * from t where exists (select 1)" "EXISTS")
      (unsupported "select * from t order by a nulls last" "NULLS")
      (unsupported "select * from t where a in (select id from b)" "subquery")
      (unsupported "select lower(a) from t" "lower()")
      (unsupported "select * from t where lower(a) = 'x'" "lower()")
      (unsupported "select sum(a) from t" "SUM()")
      (unsupported "select count(id) from t" "COUNT(*)")
      (unsupported "select count(distinct id) from t" "COUNT(*)")
      (unsupported "select a + 1 from t" "Arithmetic")
      (unsupported "select * from t where a = $1" "$1")
      (unsupported "select * from t where a = ?" "placeholder")
      (unsupported "select * from t; select * from u" "one statement")
      (unsupported "delete from t where id = 1" "Only a SELECT")
      (unsupported "UPDATE t SET a = 1" "Only a SELECT"))

    (testing "Text that isn't SQL"
      (is (= {:error "Line 1: A string is missing its closing quote." :error-type "sql-parse-error"}
             (err "select * from t where a = 'x")))
      (is (= {:error "Line 1: The SQL ends before the statement does." :error-type "sql-parse-error"}
             (err "select * from")))
      (is (= "sql-parse-error" (:error-type (err "company | where: id = 1")))))

    (testing "The line is where the reader stopped"
      (is (= "Line 3: OFFSET isn't in Pine." (:error (err "select *\nfrom t\noffset 5")))))))
