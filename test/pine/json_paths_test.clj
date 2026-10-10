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
           (sql "customer | group: data.plan => count"))))

  (testing "Two paths into one column stay apart"
    (is (re-find #"GROUP BY \"x_1\".\"data.plan\", \"x_1\".\"data.region\""
                 (:query (sql "customer | group: data.plan, data.region => count")))))

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
  (testing "and can't be filtered, sorted or grouped on"
    (doseq [e ["customer | where: data.plan = 'x'"
               "customer | o: data.plan | s: id"
               "customer | group: data.plan => count"]]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"`c_0.data` is hidden by the access policy"
                            (sql :test e [jsonb-hidden]))
          e))))

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
      (is (empty? (columns "customer | group: data.plan => count")))
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
