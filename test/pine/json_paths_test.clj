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

  (testing "Errors"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"`nope` is neither an alias nor a column of `customer`"
                          (state "customer | s: nope.x")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"`uuid_col` is not a JSON column"
                          (state "customer | s: uuid_col.x")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"can't be used on a key inside a JSON column"
                          (state "customer | s: data.created => month")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"update! can't write into a key of a JSON column"
                          (state "customer | where: id = 1 | update! data.plan = 'pro'")))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"only be compared with a value"
                          (state "customer | where: data.plan = id")))))

(deftest test-postgres
  (testing "select: the value as text, named after its path"
    (is (= {:query (str "SELECT jsonb_extract_path_text(\"c_0\".\"data\"::jsonb, ?::text, ?::text) AS \"data.address.city\", "
                        "\"c_0\".\"id\" AS \"__c_0__id\" FROM \"customer\" AS \"c_0\" LIMIT 250")
            :params ["address" "city"]}
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
    (is (= ["a" "b" "1" "c"]
           (:params (sql "customer | s: data.a | where: data.b = 1 | o: data.c")))))

  (testing "Array indexes and keys that aren't names"
    (is (= ["tags" "0" "home address" "it's"]
           (:params (sql "customer | s: data.tags[0], data.'home address', data.'it''s'")))))

  (testing "A quote inside a key can't end the column name"
    (is (re-find #"AS \"data.'a\"\"b'\"" (:query (sql "customer | s: data.'a\"b'"))))))

(deftest test-mysql
  (testing "One path parameter, with each key quoted"
    (is (= {:query (str "SELECT JSON_UNQUOTE(JSON_EXTRACT(`p_0`.`config`, ?)) AS `config.a.b`, `p_0`.`id` AS `__p_0__id` "
                        "FROM `product` AS `p_0` LIMIT 250")
            :params ["$.\"a\".\"b\""]}
           (sql :test-mysql "product | s: config.a.b")))
    (is (= ["$.\"tags\"[1]"] (:params (sql :test-mysql "product | s: config.tags[1]"))))
    (is (= ["$.\"we\\\"ird\""] (:params (sql :test-mysql "product | s: config.'we\"ird'")))))

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
    (doseq [e ["customer as c | s: c.data." "customer | w: data.address." "customer | o: data.'a'."]]
      (is (= [] (-> (state e) :hints (get (if (re-find #"w:" e) :where (if (re-find #"o:" e) :order :select)))))
          e)))
  (testing "Selecting a key doesn't use up its column"
    (is (some #{"data"} (->> (state "customer | s: data.plan, ") :hints :select (map :column))))))
