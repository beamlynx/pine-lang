(ns pine.json-paths-test
  "Keys inside a JSON column: `data.address.city`. The fixtures have
  customer.data (Postgres jsonb) and product.config (MySQL json)."
  (:require [clojure.test :refer [deftest is testing]]
            [pine.ast.main :as ast]
            [pine.eval :as eval]
            [pine.parser :as parser]))

(defn- parse [expression]
  (:result (parser/parse expression)))

(defn- state
  ([expression] (state :test expression []))
  ([connection expression policy]
   (-> expression parse (ast/generate connection nil nil {} policy))))

(defn- sql
  "{:query :params} with each param reduced to its value."
  ([expression] (sql :test expression []))
  ([connection expression] (sql connection expression []))
  ([connection expression policy]
   (-> (eval/build-query (state connection expression policy))
       (select-keys [:query :params])
       (update :params #(map :value %)))))

(deftest test-parse
  (testing "A path is the column and its keys. What comes before the first dot is only a guess at an alias"
    (is (= [{:column "data" :alias "c" :path ["plan"]}]
           (-> "s: c.data.plan" parse first :value)))
    (is (= [{:column "data" :path [0]}
            {:column "data" :path ["home address" "x"]}
            {:column "plan" :alias "data"}]
           (-> "s: data[0], data.'home address'.x, data.plan" parse first :value))))

  (testing "An apostrophe in a quoted key is written twice"
    (is (= [{:column "data" :path ["it's"]}]
           (-> "s: data.'it''s'" parse first :value))))

  (testing "A name that stops inside a path is partial"
    (is (= {:column "data" :alias "c" :path [] :json-partial true}
           (-> "customer as c | s: c.data." parse last :partial-alias)))
    (is (= {:alias "c" :column ""}
           (-> "customer as c | s: c." parse last :partial-alias))))

  (testing "`->` is not Pine"
    (is (:error (parser/parse "customer | s: data->x")))))

(deftest test-resolve
  (testing "A first name that isn't an alias is a column of the current table"
    (is (= {:alias "c_0" :column "data" :path ["plan"] :column-alias "data.plan"}
           (-> (state "customer | s: data.plan") :columns first (select-keys [:alias :column :path :column-alias])))))

  (testing "An alias is read as an alias"
    (is (= {:alias "c" :column "data" :path ["plan"]}
           (-> (state "customer as c | s: c.data.plan") :columns first (select-keys [:alias :column :path]))))
    (is (= {:alias "c" :column "data"}
           (-> (state "customer as c | s: c.data") :columns first (select-keys [:alias :column :path])))))

  (testing "The alias wins over a JSON column of the same name, and says so"
    (let [col (-> (state "user as data | customer | s: data.id") :columns first)]
      (is (= ["data" "id" nil] [(:alias col) (:column col) (:path col)]))
      (is (:alias-hides-column col))))

  (testing "`as` names a path column"
    (is (= "tier" (-> (state "customer | s: data.plan as tier") :columns first :column-alias))))

  (testing "A named result's path column is read by its name"
    (let [named-results (:pending-assignments (state "customer | s: id, data.plan |= p"))
          st (-> "p | where: data.plan = 'x' | s: data.plan" parse (ast/generate :test nil nil named-results []))]
      (is (= {:alias "p" :column "data.plan"} (-> st :columns first (select-keys [:alias :column :path]))))
      (is (re-find #"SELECT \"p\".\"data.plan\" FROM \"p\" AS \"p\" WHERE \"p\".\"data.plan\" = \?"
                   (:query (eval/build-query st))))))

  (testing "Errors"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"`customer` is a table. Name it by its alias, `c_0`"
                          (state "customer | s: customer.id")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"`nope` is neither an alias nor a column of `customer`"
                          (state "customer | s: nope.x")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"`uuid_col` is not a JSON column"
                          (state "customer | s: uuid_col.x")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"can't be used on a key inside a JSON column"
                          (state "customer | s: data.created => month")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"only be compared with a value"
                          (state "customer | where: data.plan = id")))))

(deftest test-postgres
  (testing "select: the value as text, named after its path, and its JSON type in a hidden column"
    (is (= {:query (str "SELECT jsonb_extract_path_text(\"c_0\".\"data\"::jsonb, ?::text, ?::text) AS \"data.address.city\", "
                        "\"c_0\".\"id\" AS \"__c_0__id\", "
                        "jsonb_typeof(jsonb_extract_path(\"c_0\".\"data\"::jsonb, ?::text, ?::text)) AS \"__c_0__data.address.city__type\" "
                        "FROM \"customer\" AS \"c_0\" LIMIT 250")
            :params ["address" "city" "address" "city"]}
           (sql "customer | s: data.address.city"))))

  (testing "= compares JSON values: the literal is sent as JSON"
    (is (= {:query (str "SELECT \"c_0\".\"id\" AS \"__c_0__id\", \"c_0\".* FROM \"customer\" AS \"c_0\" "
                        "WHERE jsonb_extract_path(\"c_0\".\"data\"::jsonb, ?::text) = ?::jsonb LIMIT 250")
            :params ["country" "\"SE\""]}
           (sql "customer | where: data.country = 'SE'")))
    (is (= ["active" "true"] (:params (sql "customer | where: data.active = true"))))
    (is (= ["active" "false"] (:params (sql "customer | where: data.active != false")))))

  (testing "> only compares values of the literal's JSON type"
    (is (= {:query (str "SELECT \"c_0\".\"id\" AS \"__c_0__id\", \"c_0\".* FROM \"customer\" AS \"c_0\" "
                        "WHERE (jsonb_typeof(jsonb_extract_path(\"c_0\".\"data\"::jsonb, ?::text)) = 'number' "
                        "AND jsonb_extract_path(\"c_0\".\"data\"::jsonb, ?::text) > ?::jsonb) LIMIT 250")
            :params ["seats" "seats" "10"]}
           (sql "customer | where: data.seats > 10")))
    (is (re-find #"jsonb_typeof\(.*\) = 'string'" (:query (sql "customer | where: data.signup < '2024-01-01'")))))

  (testing "like, in and is null compare text"
    (is (re-find #"jsonb_extract_path_text\(.*\) LIKE \?" (:query (sql "customer | where: data.name like 'A%'"))))
    (is (= ["plan" "a" "b"] (:params (sql "customer | where: data.plan in ('a', 'b')"))))
    (is (= ["seats" "1" "2"] (:params (sql "customer | where: data.seats in (1, 2)"))))
    (is (re-find #"jsonb_extract_path_text\(\"c_0\".\"data\"::jsonb, \?::text\) IS NULL"
                 (:query (sql "customer | where: data.cancelled_at is null")))))

  (testing "order: sorts JSON values, so numbers sort as numbers"
    (is (re-find #"ORDER BY jsonb_extract_path\(\"c_0\".\"data\"::jsonb, \?::text, \?::text\) DESC"
                 (:query (sql "customer | o: data.signup.score desc | s: id")))))

  (testing "group: groups by the path's own name"
    (is (= {:query (str "WITH \"x_1\" AS ( SELECT jsonb_extract_path_text(\"c_0\".\"data\"::jsonb, ?::text) AS \"data.plan\" "
                        "FROM \"customer\" AS \"c_0\" ) SELECT \"x_1\".\"data.plan\", COUNT(1) AS \"count\" FROM \"x_1\" "
                        "GROUP BY \"x_1\".\"data.plan\" LIMIT 10000")
            :params ["plan"]}
           (sql "customer | group: data.plan with count"))))

  (testing "Two paths into one column stay apart"
    (is (re-find #"GROUP BY \"x_1\".\"data.plan\", \"x_1\".\"data.region\""
                 (:query (sql "customer | group: data.plan, data.region with count")))))

  (testing "Params follow the order of their ? in the query: select, where, order"
    (is (= ["a" "a" "b" "1" "c"]
           (:params (sql "customer | s: data.a | where: data.b = 1 | o: data.c")))))

  (testing "Array indexes and keys that aren't names"
    (is (= ["tags" "0" "home address" "it's" "tags" "0" "home address" "it's"]
           (:params (sql "customer | s: data.tags[0], data.'home address', data.'it''s'")))))

  (testing "A quote inside a key can't end the column name"
    (is (re-find #"AS \"data.'a\"\"b'\"" (:query (sql "customer | s: data.'a\"b'"))))))

(deftest test-mysql
  (testing "One path parameter, with each key quoted"
    (is (= {:query (str "SELECT JSON_UNQUOTE(JSON_EXTRACT(`p_0`.`config`, ?)) AS `config.a.b`, `p_0`.`id` AS `__p_0__id`, "
                        "CASE JSON_TYPE(JSON_EXTRACT(`p_0`.`config`, ?)) WHEN 'INTEGER' THEN 'number' WHEN 'UNSIGNED INTEGER' THEN 'number' "
                        "WHEN 'DOUBLE' THEN 'number' WHEN 'DECIMAL' THEN 'number' ELSE LOWER(JSON_TYPE(JSON_EXTRACT(`p_0`.`config`, ?))) END "
                        "AS `__p_0__config.a.b__type` FROM `product` AS `p_0` LIMIT 250")
            :params ["$.\"a\".\"b\"" "$.\"a\".\"b\"" "$.\"a\".\"b\""]}
           (sql :test-mysql "product | s: config.a.b")))
    (is (= "$.\"tags\"[1]" (first (:params (sql :test-mysql "product | s: config.tags[1]")))))
    (is (= "$.\"we\\\"ird\"" (first (:params (sql :test-mysql "product | s: config.'we\"ird'"))))))

  (testing "= and > compare JSON values, > only within the literal's type"
    (is (re-find #"WHERE JSON_EXTRACT\(`p_0`.`config`, \?\) = CAST\(\? AS JSON\)"
                 (:query (sql :test-mysql "product | where: config.color = 'red'"))))
    (is (= {:query (str "SELECT `p_0`.`id` AS `__p_0__id`, `p_0`.* FROM `product` AS `p_0` "
                        "WHERE (JSON_TYPE(JSON_EXTRACT(`p_0`.`config`, ?)) IN ('INTEGER', 'DOUBLE', 'DECIMAL', 'UNSIGNED INTEGER') "
                        "AND JSON_EXTRACT(`p_0`.`config`, ?) > CAST(? AS JSON)) LIMIT 250")
            :params ["$.\"n\"" "$.\"n\"" "2"]}
           (sql :test-mysql "product | where: config.n > 2"))))

  (testing "is null: a missing key and a JSON null, told apart from the text 'null'"
    (is (re-find #"COALESCE\(JSON_TYPE\(JSON_EXTRACT\(`p_0`.`config`, \?\)\), 'NULL'\) = 'NULL'"
                 (:query (sql :test-mysql "product | where: config.x is null"))))
    (is (re-find #"'NULL'\) <> 'NULL'" (:query (sql :test-mysql "product | where: config.x is not null"))))))

(def ^:private jsonb-hidden
  "Hides every column whose type isn't listed, so customer.data (jsonb) is hidden."
  {:type "column-type" :allow ["integer" "uuid"] :active true})

(deftest test-access-policy
  (testing "A key inside a hidden column is hidden"
    (is (re-find #"'xxxxx' AS \"data.plan\""
                 (:query (sql :test "customer | s: data.plan" [jsonb-hidden])))))
  (testing "and can be filtered and sorted on, but not grouped on"
    (doseq [e ["customer | where: data.plan = 'x'"
               "customer | o: data.plan | s: id"]]
      (is (:query (sql :test e [jsonb-hidden])) e))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"`c_0.data` is a masked column"
                          (sql :test "customer | group: data.plan with count" [jsonb-hidden])))))

(deftest test-hints
  (testing "After an alias and a dot: its columns, as before"
    (is (= ["id" "data" "uuid_col"]
           (->> (state "customer as c | s: c.") :hints :select (map :column)))))
  (testing "Inside a JSON path: keys aren't in the schema, so nothing yet"
    (doseq [e ["customer as c | s: c.data." "customer | w: data.address." "customer | o: data.'a'."
               "customer | s: data." "customer | w: data."]]
      (is (= [] (-> (state e) :hints (get (if (re-find #"w:" e) :where (if (re-find #"o:" e) :order :select)))))
          e)))
  (testing "Selecting a key doesn't use up its column"
    (is (some #{"data"} (->> (state "customer | s: data.plan, ") :hints :select (map :column))))))

(defn- update-sql
  "The one UPDATE an expression builds, with each param reduced to its value."
  ([expression] (update-sql :test expression))
  ([connection expression]
   (-> (eval/build-query (state connection expression []))
       :queries
       first
       (select-keys [:query :params])
       (update :params #(map :value %)))))

(deftest test-update-into-a-key
  (testing "Postgres: jsonb_set, with the literal as a JSON value"
    (is (= {:query (str "UPDATE \"customer\" SET \"data\" = jsonb_set(\"data\"::jsonb, ARRAY[?::text, ?::text], ?::jsonb) "
                        "WHERE jsonb_typeof(jsonb_extract_path(\"data\"::jsonb, ?::text)) = 'array' "
                        "AND \"id\" IN ( SELECT \"c_0\".\"id\" FROM \"customer\" AS \"c_0\" WHERE \"c_0\".\"id\" = ? )")
            :params ["companies" "0" "\"c-42\"" "companies" 1]}
           (update-sql "customer | where: id = 1 | update! data.companies[0] = 'c-42'"))))

  (testing "A literal means the same JSON value as in where:"
    (is (= ["plan" "5"] (butlast (:params (update-sql "customer | where: id = 1 | update! data.plan = 5")))))
    (is (= ["plan" "-1.5"] (butlast (:params (update-sql "customer | where: id = 1 | update! data.plan = -1.5")))))
    (is (= ["plan" "true"] (butlast (:params (update-sql "customer | where: id = 1 | update! data.plan = true")))))
    (is (= ["plan" "null"] (butlast (:params (update-sql "customer | where: id = 1 | update! data.plan = null")))))
    (is (= ["plan" "\"2024-01-01\""] (butlast (:params (update-sql "customer | where: id = 1 | update! data.plan = '2024-01-01'"))))))

  (testing "The alias can name the table, and alias wins"
    (is (re-find #"SET \"data\" = jsonb_set\(\"data\"::jsonb, ARRAY\[\?::text\], \?::jsonb\) WHERE"
                 (:query (update-sql "customer as c | where: id = 1 | update! c.data.plan = 'pro'")))))

  (testing "Keys of one column nest, in the order written"
    (is (= {:query (str "UPDATE \"customer\" SET \"data\" = jsonb_set(jsonb_set(\"data\"::jsonb, ARRAY[?::text], ?::jsonb)::jsonb, "
                        "ARRAY[?::text], ?::jsonb), \"name\" = ? "
                        "WHERE jsonb_typeof(\"data\"::jsonb) = 'object' "
                        "AND \"id\" IN ( SELECT \"c_0\".\"id\" FROM \"customer\" AS \"c_0\" WHERE \"c_0\".\"id\" = ? )")
            :params ["a" "1" "b" "2" "x" 1]}
           (update-sql "customer | where: id = 1 | update! data.a = 1, name = 'x', data.b = 2"))))

  (testing "A Postgres json column gets the jsonb back as json"
    (is (re-find #"SET \"config\" = jsonb_set\(\"config\"::jsonb, ARRAY\[\?::text\], \?::jsonb\)::json WHERE"
                 (:query (update-sql "product | where: id = 1 | update! config.color = 'red'")))))

  (testing "MySQL: JSON_SET with one path parameter"
    (is (= {:query (str "UPDATE `product` SET `config` = JSON_SET(`config`, ?, CAST(? AS JSON)) "
                        "WHERE JSON_TYPE(JSON_EXTRACT(`config`, ?)) = 'ARRAY' "
                        "AND `id` IN ( SELECT * FROM ( SELECT `p_0`.`id` FROM `product` AS `p_0` WHERE `p_0`.`id` = ? ) AS `pine_sub` )")
            :params ["$.\"tags\"[0]" "\"x\"" "$.\"tags\"" 1]}
           (update-sql :test-mysql "product | where: id = 1 | update! config.tags[0] = 'x'"))))

  (testing "SQLite: json_set, reading the value as JSON"
    (is (re-find #"SET \"data\" = json_set\(\"data\", \?, json\(\?\)\) WHERE"
                 (:query (update-sql :test-sqlite "customer | where: id = 1 | update! data.plan = 'pro'")))))

  (testing "Refused"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"can only write a value into a key"
                          (state "customer | where: id = 1 | update! data.plan = name")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"can't write `data` and a key inside it at once"
                          (state "customer | where: id = 1 | update! data = '{}', data.plan = 'pro'")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"`uuid_col` is not a JSON column"
                          (state "customer | where: id = 1 | update! uuid_col.x = 'pro'")))))

(deftest test-json-type-column
  (let [columns #(->> (state %) :columns (filter :json-type-of))]
    (testing "One hidden type column per selected path, linked to it by name"
      (is (= [{:column "data" :alias "c_0" :path ["plan"] :column-alias "__c_0__data.plan__type"
               :hidden true :json-type-of "data.plan"}]
             (map #(dissoc % :operation-index) (columns "customer | s: data.plan")))))

    (testing "None where there is nothing to edit"
      (is (empty? (columns "customer | group: data.plan with count")))
      (is (empty? (columns "customer | s: data.plan | count:"))))

    (testing "None for a JSON column the access policy hides"
      (is (empty? (->> (state :test "customer | s: data.plan" [{:column "data"}]) :columns (filter :json-type-of)))))

    (testing "A named result leaves it out"
      (let [{:keys [named-results]} (reduce (fn [{:keys [named-results]} expr]
                                              (let [s (-> expr parse (ast/generate :test nil nil named-results []))]
                                                {:named-results (merge named-results (:pending-assignments s))
                                                 :last s}))
                                            {:named-results {}}
                                            ["customer | s: data.plan |= p"])
            q (-> "p | s: data.plan" parse (ast/generate :test nil nil named-results []) eval/build-query :query)]
        (is (not (re-find #"__type" q)))))))

(deftest test-join-on-array-elements
  (testing "A side of a join pair can be a path into a JSON array"
    (is (= [{:type :table :value {:table "employee"
                                  :join-column-pairs [{:left "data" :right "id" :left-path ["staff" :* "id"]}]}}]
           (rest (parse "customer | employee .id = .data.staff[*].id")))))

  (testing "`[*]` is only read in a join for now"
    (is (:error (parser/parse "customer | s: data[*].id"))))

  (testing "Postgres tests containment, with each key a parameter"
    (is (= {:query (str "SELECT \"c_0\".\"id\" AS \"__c_0__id\", \"e_1\".\"id\" AS \"__e_1__id\", \"e_1\".* "
                        "FROM \"customer\" AS \"c_0\" JOIN \"employee\" AS \"e_1\" "
                        "ON \"c_0\".\"data\"::jsonb @> jsonb_build_array(jsonb_build_object(?::text, \"e_1\".\"id\")) LIMIT 250")
            :params ["id"]}
           (sql "customer | employee .id = .data[*].id")))
    (is (= {:query (str "SELECT \"c_0\".\"id\" AS \"__c_0__id\", \"e_1\".\"id\" AS \"__e_1__id\", \"e_1\".* "
                        "FROM \"customer\" AS \"c_0\" JOIN \"employee\" AS \"e_1\" "
                        "ON \"c_0\".\"data\"::jsonb @> jsonb_build_array(\"e_1\".\"id\") LIMIT 250")
            :params []}
           (sql "customer | employee .id = .data[*]"))))

  (testing "Keys before `[*]` lead to the array"
    (is (re-find #"ON jsonb_extract_path\(\"c_0\".\"data\"::jsonb, \?::text\) @> jsonb_build_array\(jsonb_build_object\(\?::text, \"e_1\".\"id\"\)\)"
                 (:query (sql "customer | employee .id = .data.staff[*].id")))))

  (testing "The path may be the new table's side"
    (is (re-find #"ON jsonb_extract_path\(\"c_1\".\"data\"::jsonb, \?::text\) @> jsonb_build_array\(jsonb_build_object\(\?::text, \"e_0\".\"id\"\)\)"
                 (:query (sql "employee | customer .data.staff[*].id = .id")))))

  (testing "A cast on the other side applies to the value looked for"
    (is (re-find #"jsonb_build_object\(\?::text, \"e_1\".\"id\"::text\)"
                 (:query (sql "customer | employee .id ::text = .data[*].id")))))

  (testing "The join's parameters come between the SELECT's and the WHERE's"
    (is (= ["plan" "plan" "staff" "id" 3]
           (:params (sql "customer | employee .id = .data.staff[*].id | where: id = 3 | s: c_0.data.plan")))))

  (testing "MySQL"
    (is (= {:query (str "SELECT `p_0`.`id` AS `__p_0__id`, `e_1`.`id` AS `__e_1__id`, `e_1`.* "
                        "FROM `product` AS `p_0` JOIN `employee` AS `e_1` "
                        "ON JSON_CONTAINS(JSON_EXTRACT(`p_0`.`config`, ?), JSON_ARRAY(`e_1`.`id`)) LIMIT 250")
            :params ["$.\"staff\"[*].\"id\""]}
           (sql :test-mysql "product | employee .id = .config.staff[*].id"))))

  (testing "SQLite"
    (is (= {:query (str "SELECT \"p_0\".\"id\" AS \"__p_0__id\", \"e_1\".\"id\" AS \"__e_1__id\", \"e_1\".* "
                        "FROM \"product\" AS \"p_0\" JOIN \"employee\" AS \"e_1\" "
                        "ON EXISTS (SELECT 1 FROM json_each(\"p_0\".\"config\", ?) AS \"pine_each\" "
                        "WHERE CASE WHEN \"pine_each\".\"type\" IN ('object', 'array') "
                        "THEN json_extract(\"pine_each\".\"value\", ?) END = \"e_1\".\"id\") LIMIT 250")
            :params ["$.\"staff\"" "$.\"id\""]}
           (sql :test-sqlite "product | employee .id = .config.staff[*].id")))
    (is (re-find #"WHERE \"pine_each\".\"value\" = \"e_1\".\"id\"\)"
                 (:query (sql :test-sqlite "product | employee .id = .config[*]")))))

  (testing "Errors"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"`employee` has no JSON column `data`; `customer` does. The first column of a join is the new table's: write `.id = .data.staff\[\*\].id`"
                          (state "customer | employee .data.staff[*].id = .id")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"`data.staff` needs `\[\*\]`"
                          (state "customer | employee .id = .data.staff")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"more than one `\[\*\]`"
                          (state "customer | employee .id = .data[*].x[*]")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"an index after `\[\*\]`"
                          (state "customer | employee .id = .data[*][0]")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"A cast can't follow a JSON path"
                          (state "customer | employee .id = .data[*].id ::text")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"`uuid_col` is not a JSON column"
                          (state "customer | employee .id = .uuid_col[*]")))))

(deftest test-join-on-array-elements-access-policy
  (testing "A masked JSON column can be joined on: the policy masks what comes back, and it stays masked"
    (is (re-find #"'xxxxx' AS \"data\""
                 (:query (eval/build-query (state :test "customer | employee .id = .data[*].id | s: c_0.data, e_1.id" [jsonb-hidden])))))))

(defn- sql-without-positions
  "As sql, without the hidden columns that let the grid edit an element: its
  position and its JSON type. test-element-positions tests those."
  ([expression] (sql-without-positions :test expression))
  ([connection expression]
   (let [elements (fn [st] (set (keep #(when (:elements %) (:alias %)) (vals (:aliases st)))))]
     (-> (let [st (state connection expression [])
               e (elements st)]
           (update st :columns (fn [cs] (vec (remove #(or (:element-index %) (and (:json-type-of %) (e (:alias %)))) cs)))))
         eval/build-query
         (select-keys [:query :params])
         (update :params #(map :value %))))))

(deftest test-array-elements
  (testing "`unnest: a.data as e` parses as a step, written like a column"
    (is (= {:type :elements :value {:alias "c" :column "data" :path ["staff"] :as "s"}}
           (last (parse "customer as c | unnest: c.data.staff as s"))))
    (is (= {:type :elements :value {:column "data" :path []}}
           (last (parse "customer | unnest: data"))))
    (is (:error (parser/parse "customer | data[*] as e")) "`[*]` alone isn't a step: it reads as a join")
    (is (:error (parser/parse "customer | unnest: data[*] as e")) "`unnest:` takes the array, without `[*]`"))

  (testing "One row for each element, joined to its row"
    (is (= {:query (str "SELECT \"c\".\"id\", jsonb_extract_path_text(\"s\".\"value\"::jsonb, ?::text) AS \"name\", "
                        "\"c\".\"id\" AS \"__c__id\" FROM \"customer\" AS \"c\" CROSS JOIN LATERAL jsonb_array_elements("
                        "CASE WHEN jsonb_typeof(jsonb_extract_path(\"c\".\"data\"::jsonb, ?::text)) = 'array' "
                        "THEN jsonb_extract_path(\"c\".\"data\"::jsonb, ?::text) END) AS \"s\"(\"value\") "
                        "WHERE jsonb_extract_path(\"s\".\"value\"::jsonb, ?::text) = ?::jsonb LIMIT 250")
            :params ["name" "staff" "staff" "role" "\"admin\""]}
           (sql-without-positions "customer as c | unnest: c.data.staff as s | where: s.role = 'admin' | s: c.id, s.name"))))

  (testing "`e` is the element, `e.k` a key, and a bare name a column of the table"
    (is (re-find #"^SELECT \"c_0\".\"id\", jsonb_extract_path_text\(\"e\".\"value\"::jsonb, \?::text\) AS \"k\""
                 (:query (sql-without-positions "customer | unnest: data as e | s: id, e.k"))))
    (is (re-find #"\(\"e\".\"value\"::jsonb #>> '\{\}'\) AS \"e\""
                 (:query (sql-without-positions "customer | unnest: data as e | s: e.*")))))

  (testing "unnest: adds `e` to the result, in place of its table's columns, until a select: after it"
    (let [columns (fn [expression] (second (re-find #"^SELECT (.*?) FROM" (:query (sql-without-positions expression)))))
          e "(\"e\".\"value\"::jsonb #>> '{}') AS \"e\""]
      (is (= (str "\"c_0\".\"id\" AS \"__c_0__id\", " e)
             (columns "customer | unnest: data as e")))
      (is (= (str "\"c_0\".\"id\", \"c_0\".\"id\" AS \"__c_0__id\", " e)
             (columns "customer | s: id | unnest: data as e")))
      (is (= "\"c_0\".\"id\", jsonb_extract_path_text(\"e\".\"value\"::jsonb, ?::text) AS \"k\", \"c_0\".\"id\" AS \"__c_0__id\""
             (columns "customer | s: id | unnest: data as e | s: e.k")))
      (is (= "jsonb_extract_path_text(\"e\".\"value\"::jsonb, ?::text) AS \"k\", \"c_0\".\"id\" AS \"__c_0__id\""
             (columns "customer | unnest: data as e | s: e.k")))
      (is (= (str "\"c_0\".\"id\" AS \"__c_0__id\", " e)
             (columns "customer | unnest: data as e | where: e = 'x' | order: id")))
      (testing "Only the inner elements of elements"
        (is (re-find #"AS \"i\"$" (columns "customer | unnest: data.orders as o | unnest: o.items as i"))))))

  (testing "An element compares as JSON, like a key"
    (is (re-find #"WHERE \"e\".\"value\"::jsonb = \?::jsonb"
                 (:query (sql-without-positions "customer | unnest: data as e | where: e = 'x'")))))

  (testing "Elements of elements, and a default name"
    (is (re-find #"AS \"i\"\(\"value\"\) CROSS JOIN LATERAL jsonb_array_elements\(CASE WHEN jsonb_typeof\(jsonb_extract_path\(\"i\".\"value\"::jsonb"
                 (:query (sql-without-positions "customer as c | unnest: data.items as i | unnest: i.parts as p | s: p.name"))))
    (is (re-find #"AS \"s_1\"\(\"value\"\)" (:query (sql-without-positions "customer | unnest: data.staff")))))

  (testing "A join after the elements is from the table the pipe is on, and comes after them in the SQL"
    (let [{:keys [query]} (sql-without-positions "customer as c | unnest: data as e | employee .id = .id")]
      (is (re-find #"AS \"e\"\(\"value\"\) JOIN \"employee\" AS \"e_\d\" ON \"c\".\"id\" = \"e_\d\".\"id\"" query))
      (is (re-find #"\"e_\d\"\.\*" query) "The joined table shows its columns")
      (is (re-find #"AS \"e\", " query) "and the elements are still in the result")))

  (testing "MySQL reads the array with JSON_TABLE"
    (is (= {:query (str "SELECT JSON_UNQUOTE(`t`.`value`) AS `t`, `p_0`.`id` AS `__p_0__id` FROM `product` AS `p_0` "
                        "CROSS JOIN JSON_TABLE(CASE WHEN JSON_TYPE(JSON_EXTRACT(`p_0`.`config`, ?)) = 'ARRAY' "
                        "THEN JSON_EXTRACT(`p_0`.`config`, ?) END, '$[*]' COLUMNS (`value` JSON PATH '$')) AS `t` LIMIT 250")
            :params ["$.\"tags\"" "$.\"tags\""]}
           (sql-without-positions :test-mysql "product | unnest: config.tags as t | s: t"))))

  (testing "SQLite reads it with json_each, and writes each element as JSON text"
    (let [{:keys [query]} (sql-without-positions :test-sqlite "customer | unnest: data.staff as s | s: s.name")]
      (is (re-find #"CROSS JOIN json_each\(CASE WHEN json_type\(\"c_0\".\"data\", \?\) = 'array' THEN json_extract\(\"c_0\".\"data\", \?\) END\) AS \"s\"" query))
      (is (re-find #"json_extract\(\(CASE \"s\".\"type\" WHEN 'object' THEN \"s\".\"value\" .* ELSE json_quote\(\"s\".\"value\"\) END\), \?\) AS \"name\"" query))))

  (testing "Errors"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"a table comes first"
                          (state "unnest: data as e")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"`uuid_col` is not a JSON column, so `unnest: uuid_col` has no elements"
                          (state "customer | unnest: uuid_col as e")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"`c` is already an alias"
                          (state "customer as c | unnest: data as c")))
    (is (state "customer | unnest: data as e | ") "A table still being typed isn't refused"))

  (testing "delete! changes the table's rows that have a matching element"
    (is (re-find #"^DELETE FROM \"customer\" WHERE \"id\" IN \( SELECT .* CROSS JOIN LATERAL jsonb_array_elements"
                 (:query (sql-without-positions "customer | unnest: data as e | where: e.x = 1 | delete! .id"))))))

(deftest test-array-elements-access-policy
  (testing "An element, and every key of one, is masked when its column is"
    (let [{:keys [query]} (sql :test "customer | unnest: data as e | s: id, e.k" [jsonb-hidden])]
      (is (re-find #"'xxxxx' AS \"k\"" query))))
  (testing "and can be filtered on, but not grouped on"
    (is (:query (sql :test "customer | unnest: data as e | where: e.k = 'x' | s: id" [jsonb-hidden])))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"`c_0.data` is a masked column"
                          (sql :test "customer | unnest: data as e | group: e.k with count" [jsonb-hidden])))))

(deftest test-unnest-hints-and-canvas
  (testing "unnest: is offered the JSON columns of the table the pipe is on, or of an alias"
    (is (= [{:column "data" :alias "c_0"}] (get-in (state "customer | unnest: ") [:hints :unnest])))
    (is (= [{:column "data" :alias "c"}] (get-in (state "customer as c | unnest: c.") [:hints :unnest])))
    (is (= [{:column "data" :alias "c_0"}] (get-in (state "customer | unnest: da") [:hints :unnest])))
    (is (= [] (get-in (state "customer | unnest: data.x") [:hints :unnest])) "Keys aren't in the schema")
    (is (= [] (get-in (state "customer | unnest: data.") [:hints :unnest]))))

  (testing "A table with no JSON column gets none, except on SQLite, which keeps JSON in text"
    (is (= [] (get-in (state "report | unnest: ") [:hints :unnest])))
    (is (= ["id" "title"]
           (map :column (get-in (state :test-sqlite "report | unnest: " []) [:hints :unnest])))))

  (testing "The pipe stays on the table. The elements aren't a table node: the canvas gets them as :unnests"
    (let [s (state "customer as c | unnest: data.orders as o | unnest: o.items as i | s: i.sku")]
      (is (= "c" (:current s)))
      (is (= ["c"] (map :alias (:selected-tables s))))
      (is (= [{:alias "o" :table "c" :text "data.orders"} {:alias "i" :table "c" :text "o.items"}]
             (map #(select-keys % [:alias :table :text]) (:unnests s))))))
  (testing "It shows on the node of the table whose column holds the array"
    (is (= [{:alias "e" :table "c" :text "data"}]
           (map #(select-keys % [:alias :table :text]) (:unnests (state "customer as c | employee .id = .id | unnest: c.data as e")))))))

(deftest test-element-positions
  (let [hidden (fn [expression]
                 (->> (state expression) :columns (filter :hidden)
                      (map #(select-keys % [:alias :column-alias :element-index :json-type-of :path]))))]
    (testing "Each element's position comes back hidden, from 0, with the JSON type of each value the grid can edit"
      (is (= [{:alias "c_0" :column-alias "__c_0__id"}
              {:alias "e" :column-alias "__e__index" :element-index true}
              {:alias "e" :column-alias "__e__k__type" :json-type-of "k" :path ["k"]}]
             (hidden "customer | unnest: data as e | s: id, e.k")))
      (is (re-find #"\(\"e\".\"pine_index\" - 1\) AS \"__e__index\""
                   (:query (sql "customer | unnest: data as e | s: id, e.k"))))
      (is (re-find #"jsonb_array_elements\(.*\) WITH ORDINALITY AS \"e\"\(\"value\", \"pine_index\"\)"
                   (:query (sql "customer | unnest: data as e | s: id, e.k")))))

    (testing "`e` shown by itself gets a type too"
      (is (some #(= {:alias "e" :column-alias "__e__e__type" :json-type-of "e" :path []} %)
                (hidden "customer | unnest: data as e"))))

    (testing "Elements of elements: each level has its position"
      (is (= ["__o__index" "__i__index"]
             (map :column-alias (filter :element-index (hidden "customer | unnest: data as o | unnest: o.items as i | s: i.x")))))
      (is (= [{:alias "o" :source "c_0" :column "data" :path []}
              {:alias "i" :source "o" :column nil :path ["items"]}]
             (map #(select-keys % [:alias :source :column :path])
                  (:unnests (state "customer | unnest: data as o | unnest: o.items as i | s: i.x"))))))

    (testing "MySQL numbers them with FOR ORDINALITY, SQLite with json_each's key"
      (is (re-find #"COLUMNS \(`value` JSON PATH '\$', `pine_index` FOR ORDINALITY\)"
                   (:query (sql :test-mysql "product | unnest: config.tags as t | s: t"))))
      (is (re-find #"\"s\".\"key\" AS \"__s__index\""
                   (:query (sql :test-sqlite "customer | unnest: data.staff as s | s: s.name")))))

    (testing "Not when no row can be edited, as after group:, and not under a masked column"
      (is (empty? (hidden "customer | unnest: data as e | group: e.k with count")))
      (is (not-any? :element-index (:columns (state :test "customer | unnest: data as e | s: id, e.k" [jsonb-hidden])))))

    (testing "Hidden columns don't hide `e`: it shows until a select: after the unnest:"
      (is (re-find #"AS \"e\" FROM" (:query (sql "customer | unnest: data as e")))))))
