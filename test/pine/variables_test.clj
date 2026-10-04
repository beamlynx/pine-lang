(ns pine.variables-test
  "$variables: values supplied with the request (pine.variables)."
  (:require [clojure.test :refer [deftest is testing]]
            [pine.api :as api]
            [pine.ast.main :as ast]
            [pine.data-types :as dt]
            [pine.eval :as eval]
            [pine.parser :as parser]
            [pine.variables :as v]))

(defn- generate
  "SQL for one expression, with these $variable values."
  ([expression] (generate expression {}))
  ([expression bindings]
   (-> expression
       parser/parse
       :result
       (v/bind bindings)
       (ast/generate :test nil nil {} [])
       eval/build-query)))

(deftest test-parse
  (testing "a $variable is a value, wherever a literal can go"
    (is (= [(dt/column "name") "=" (dt/variable "company_name")]
           (-> "company | where: name = $company_name" parser/parse :result second :value)))
    (is (= [(dt/column "country") "IN" (dt/variable "countries" true)]
           (-> "company | where: country in $countries" parser/parse :result second :value)))
    (is (= [(dt/column "country") "NOT IN" (dt/variable "countries" true)]
           (-> "company | where: country not in $countries" parser/parse :result second :value)))
    (is (= {:column {:alias nil :column "name"} :value (dt/variable "new_name")}
           (-> "company | where: id = 1 | update! name = $new_name" parser/parse :result last :value :assignments first))))

  (testing "`= $` while the name is being typed reads as a half-written condition"
    (is (= {:column "name" :operator :equals}
           (-> "company | where: name = $" parser/parse :result last :value :partial-condition))))

  (testing "a $variable can't follow `is`, which only takes null"
    (is (thrown-with-msg? Exception #"can't follow `is`" (parser/parse "company | where: name is $x")))))

(deftest test-dollar-in-a-literal
  (testing "a `$` inside a string literal shows in the SQL preview instead of breaking it"
    (is (re-find #"'price \$5'" (eval/formatted-query (generate "company | where: name = 'price $5'"))))))

(deftest test-values-are-parameters
  (testing "a bound value produces the same SQL and parameters as the literal"
    (is (= (generate "company | where: name = 'Acme Inc.'")
           (generate "company | where: name = $n" {"n" "Acme Inc."})))
    (is (= (generate "company | where: id = 1")
           (generate "company | where: id = $id" {"id" 1})))
    (is (= (generate "company | where: country in ('PK' 'DK')")
           (generate "company | where: country in $c" {"c" ["PK" "DK"]})))
    (is (= (generate "company | where: name = 'Acme Inc.' ::text")
           (generate "company | where: name = $n ::text" {"n" "Acme Inc."}))))

  (testing "a value that looks like SQL stays a parameter"
    (let [{:keys [query params]} (generate "company | where: name = $n" {"n" "x' or 1=1 --"})]
      (is (re-find #"\"name\" = \? " query))
      (is (not (re-find #"1=1" query)))
      (is (= [(dt/string "x' or 1=1 --")] params))))

  (testing "a value is typed by its column, like a literal"
    (is (= [(dt/number "7")] (:params (generate "company | where: id = $id" {"id" "7"})))))

  (testing "in update! too"
    (is (= (eval/build-query (-> "company | where: id = 1 | update! name = 'x'" parser/parse :result (ast/generate :test nil nil {} [])))
           (generate "company | where: id = $id | update! name = $name" {"id" 1 "name" "x"})))))

(deftest test-unbound
  (testing "with no value, the expression still builds and the preview shows $name"
    (let [built (generate "company | where: name = $n | where: country in $c")]
      (is (re-find #"\"name\" = \? .*\"country\" IN \(\?\)" (:query built)))
      (is (re-find #"\$n.*\$c" (eval/formatted-query built)))))

  (testing "only the variables with a value are replaced"
    (is (= ["c"] (v/used (v/bind (:result (parser/parse "company | where: name = $n | where: country = $c")) {"n" "x"})))))

  (testing "used lists each variable once, in order"
    (is (= ["a" "b"] (v/used (:result (parser/parse "company | where: id = $a | where: name = $b | where: country = $a")))))))

(deftest test-wrong-shape
  (testing "a list where one value goes, and one value where a list goes, say how to fix it"
    (is (thrown-with-msg? Exception #"\$c is a list, but it's used where one value goes. Use `in \$c`"
                          (generate "company | where: country = $c" {"c" ["PK"]})))
    (is (thrown-with-msg? Exception #"\$c is used with `in`, so it needs a list"
                          (generate "company | where: country in $c" {"c" "PK"})))
    (is (thrown-with-msg? Exception #"\$c is an empty list"
                          (generate "company | where: country in $c" {"c" []})))))

(deftest test-normalize
  (testing "accepts {:name {:value v}} with keyword or string keys"
    (is (= {"n" "x" "ids" [1 2] "on" true} (v/normalize {:n {:value "x"} "ids" {:value [1 2]} :on {:value true}})))
    (is (= {} (v/normalize nil))))

  (testing "refuses what can't be a value, with a reason"
    (doseq [[raw pattern] [[{:n {:value nil}} #"can't be null"]
                           [{:n {:value {:a 1}}} #"must be a string, a number or a boolean"]
                           [{:n {:expression "company"}} #"isn't supported yet"]
                           [{:n "x"} #"must be passed as"]
                           [{(keyword "1x") {:value 1}} #"isn't a valid variable name"]
                           [{:n {:value (vec (range 5001))}} #"more than 5000 values"]
                           [(into {} (for [i (range 51)] [(keyword (str "v" i)) {:value 1}])) #"At most 50"]
                           [["not" "a" "map"] #"must be an object"]]]
      (is (thrown-with-msg? Exception pattern (v/normalize raw)) (pr-str raw)))))

(deftest test-api-build-reports-variables
  (testing "/build lists the variables used and those without a value, and still builds"
    (binding [v/*bindings* {"n" "Acme"}]
      (let [response (api/api-build ["company | where: name = $n | where: country in $c"] nil :test)]
        (is (nil? (:error response)))
        (is (= {:used ["n" "c"] :unbound ["c"] :lists ["c"]} (:variables response)))
        (is (re-find #"'Acme'.*\$c" (:query response))))))

  (testing "variables in earlier blocks count, and get their values there too"
    (binding [v/*bindings* {"n" "Acme"}]
      (let [response (api/api-build ["company | where: name = $n |= acme" "acme | employee"] nil :test)]
        (is (nil? (:error response)))
        (is (= {:used ["n"] :unbound [] :lists []} (:variables response)))))))

(defn- post
  "Call a route with already-parsed params, as wrap-json-params would hand
  them over. Calling app-routes directly (not app) is what lets the
  connection be the :test fixture keyword, which a JSON body can't carry."
  [uri params]
  (:body (api/app-routes {:request-method :post :uri uri :params params})))

(deftest test-eval-route
  (testing "/eval refuses to run with a variable that has no value, naming each one"
    (let [response (post "/api/v1/eval" {:expressions ["company | where: name = $n | where: id = $id"]
                                         :connection-id :test
                                         :variables {:n {:value "Acme"}}})]
      (is (= "unbound-variable" (:error-type response)))
      (is (= ["id"] (:unbound response)))
      (is (= "No value for $id." (:error response)))))

  (testing "with every value, it gets as far as running"
    ;; The :test fixture has a schema but no connection pool, so a query
    ;; that reaches the database fails with "Connection not found". That's
    ;; the point: it got past every variable check first.
    (let [response (post "/api/v1/eval" {:expressions ["company | where: name = $n"]
                                         :connection-id :test
                                         :variables {:n {:value "Acme"}}})]
      (is (nil? (:error-type response)))
      (is (re-find #"(?i)connection" (str (:error response))))))

  (testing "a malformed variables map is reported, not thrown"
    (let [response (post "/api/v1/build" {:expressions ["company"]
                                          :connection-id :test
                                          :variables {:n {:value nil}}})]
      (is (= "variables" (:error-type response)))
      (is (re-find #"can't be null" (:error response))))))

(deftest test-eval-route-reports-parse-errors-as-before
  (testing "an expression the parser throws on gets the usual error from /eval, not an unhandled one"
    (doseq [expression ["company | where: name is $x" "company | where: name is 'x'"]]
      (let [response (post "/api/v1/eval" {:expressions [expression] :connection-id :test})
            direct (api/api-eval [expression] :test [])]
        (is (string? (:error response)) expression)
        (is (not= "unbound-variable" (:error-type response)) expression)
        (is (= (:error direct) (:error response)) expression)))))

(deftest test-used-ignores-expressions-that-dont-parse
  (testing "an expression the parser throws on, or rejects, contributes no variables"
    (is (= [] (vec (v/used-in-expressions ["company | where: name is $x"]))))
    (is (= [] (vec (v/used-in-expressions ["company | where: name is 'x'" "|||"]))))
    (is (= ["n"] (vec (v/used-in-expressions ["company | where: name is $x" "company | where: name = $n"]))))))

(deftest test-bind-leaves-other-queries-alone
  (testing "a value for a variable that isn't used changes nothing"
    ;; The UI sends `variables` with every request, so every query goes
    ;; through the binding walk, not only ones with a $variable.
    (doseq [expression ["company | where: id = 1 or id = 2"
                        "company | where: country in ('PK' 'DK')"
                        "company | where: name = 'Acme Inc.' ::text"
                        "company | where: id = 1 | update! name = 'x'"
                        "company | s: id, name | l: 5"]]
      (is (= (generate expression) (generate expression {"unused" 1})) expression)))

  (testing "nor across blocks with a named result"
    (binding [v/*bindings* {"unused" 1}]
      (let [with (api/api-build ["company | where: id = 1 |= x" "x | employee"] nil :test)]
        (binding [v/*bindings* {}]
          (is (= (:query (api/api-build ["company | where: id = 1 |= x" "x | employee"] nil :test))
                 (:query with))))))))

;; ------------
;; VALUES BLOCKS
;; ------------

(deftest test-values-blocks
  (testing "a block of only `$name = value` lines is a values block"
    (is (v/values-block? "$a = 'x'"))
    (is (v/values-block? "-- the company\n/* and more */\n$a = 'x'"))
    (is (not (v/values-block? "company | where: name = $a")))
    (is (not (v/values-block? ""))))

  (testing "it reads strings, numbers, booleans and lists, with comments between"
    (is (= {"company_name" "Acme" "statuses" ["failed" "stuck"] "n" 42 "ratio" 0.5 "on" false "since" "2026-09-01"}
           (v/parse-values-block "$company_name = 'Acme'\n$statuses = ('failed', 'stuck')\n-- numbers\n$n = 42\n$ratio = 0.5\n$on = false\n$since = '2026-09-01'"))))

  (testing "several blocks combine, later ones winning"
    (is (= {"a" "y" "b" [1 2]} (v/text-values ["$a = 'x'" "company" "$a = 'y'\n$b = (1, 2)"]))))

  (testing "a query in the same block says to split them"
    (is (thrown-with-msg? Exception #"Put a blank line between the values and the query"
                          (v/parse-values-block "$a = 'x'\ncompany | where: name = $a"))))

  (testing "anything else that isn't `$name = value` says what a line should look like"
    (is (thrown-with-msg? Exception #"Each line is `\$name = value`" (v/parse-values-block "$a = ")))
    (is (thrown-with-msg? Exception #"Each line is `\$name = value`" (v/parse-values-block "$a = null")))))

(deftest test-values-blocks-in-requests
  (testing "/build uses the values written in the text, and reports them"
    (let [response (post "/api/v1/build" {:expressions ["$n = 'Acme'\n$c = ('PK', 'DK')" "company | where: name = $n | where: country in $c"]
                                          :connection-id :test})]
      (is (nil? (:error response)))
      (is (= {:used ["n" "c"] :unbound [] :lists ["c"] :values {"n" "Acme" "c" ["PK" "DK"]}} (:variables response)))
      (is (re-find #"'Acme'.*'PK', 'DK'" (:query response)))))

  (testing "a value passed in the request overrides the one written in the text"
    (let [response (post "/api/v1/build" {:expressions ["$n = 'Acme'" "company | where: name = $n"]
                                          :connection-id :test
                                          :variables {:n {:value "Globex"}}})]
      (is (re-find #"'Globex'" (:query response)))))

  (testing "/eval counts written values as given, and still names the rest"
    (let [response (post "/api/v1/eval" {:expressions ["$n = 'Acme'" "company | where: name = $n | where: id = $id"]
                                         :connection-id :test})]
      (is (= "unbound-variable" (:error-type response)))
      (is (= ["id"] (:unbound response)))))

  (testing "with every value written, eval gets as far as running"
    (let [response (post "/api/v1/eval" {:expressions ["$n = 'Acme'" "company | where: name = $n"]
                                         :connection-id :test})]
      (is (nil? (:error-type response)))
      (is (re-find #"(?i)connection" (str (:error response))))))

  (testing "a values block after the query still applies, and values blocks alone build like an empty tab"
    (is (re-find #"'Acme'" (:query (post "/api/v1/build" {:expressions ["company | where: name = $n" "$n = 'Acme'"] :connection-id :test}))))
    (is (nil? (:error (post "/api/v1/build" {:expressions ["$n = 'Acme'"] :connection-id :test})))))

  (testing "a broken values block is reported, not thrown"
    (let [response (post "/api/v1/build" {:expressions ["$n = 'Acme'\ncompany"] :connection-id :test})]
      (is (= "variables" (:error-type response)))
      (is (re-find #"blank line" (:error response))))))
