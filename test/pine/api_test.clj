(ns pine.api-test
  (:require [cheshire.core :as json]
            [clojure.test :refer [deftest is testing]]
            [pine.api :as api]))

(defn- assert-clean-table [table]
  (is (= #{:schema :table :alias} (set (keys table)))
      "table entries must not carry :ast (a variable's own full state snapshot)"))

(deftest test-api-build-empty-expression
  (testing "an empty/blank last expression still returns table hints instead of short-circuiting"
    ;; Regression test: api-build used to short-circuit on any blank last-expr
    ;; (str/blank?), which also swallowed "" - even though only nil actually
    ;; crashes the parser. That meant pressing Tab on an empty input showed
    ;; no table hints at all.
    (doseq [expressions [[] [""] ["   "]]]
      (let [response (api/api-build expressions nil :test)]
        (is (nil? (:error response)))
        (is (seq (get-in response [:ast :hints :table]))
            (str "expected table hints for expressions " (pr-str expressions)))))))

;; These call the real public entry point (api/api-build), not the private
;; prune-ast helper directly. Testing prune-ast in isolation only proves the
;; helper itself behaves correctly — it proves nothing about whether api-build
;; still routes its result through prune-ast at all, so a future edit that
;; forgets to call it, passes the wrong state, or leaks a new field through
;; some other path would go uncaught. Calling api-build with connection-id
;; :test is what makes this possible: :test is a shared sentinel
;; (pine.db.connections/test-connection-ids) that both the schema lookup
;; (pine.db.main) and the connection-name lookup (connections.clj) recognize,
;; so api-build's connections/get-connection-name call — which normally
;; requires a real registered connection pool — succeeds without one.
(deftest test-api-build-ast
  (let [single-block  (:ast (api/api-build
                             ["tenant as t | company .tenantId | group: t.title |= x"]
                             nil :test))
        chained-blocks (:ast (api/api-build
                              ["tenant as t | company .tenantId | group: t.title |= x"
                               "x | s: count, | o: count desc |= y"
                               "y | s: count, |= z"
                               "z | "]
                              nil :test))]

    (testing "ast.named-results entries are pruned like pending-assignments, not raw snapshots"
      ;; A raw variable snapshot carries :named-results and :references from
      ;; pre-handle/post-handle. Left unpruned, each chained |= re-embeds every
      ;; earlier variable's own full snapshot inside the new one, growing the
      ;; response payload superlinearly with the number of chained expressions
      ;; instead of linearly.
      (is (= #{"x" "y" "z"} (set (keys (:named-results chained-blocks)))))
      (doseq [[name var-ast] (:named-results chained-blocks)]
        (testing (str "variable " name)
          (is (= #{:tables :selected-tables :joins :columns} (set (keys var-ast)))
              "should only carry the fields VariableAst (client.ts) actually uses")
          (is (not (contains? var-ast :named-results))
              "must not recursively embed earlier variables' own snapshots")
          (is (not (contains? var-ast :references))
              "must not carry the full schema references map"))))

    (testing "table entries (top-level and nested inside variables) never carry :ast"
      ;; A variable-backed table entry carries a full :ast (the variable's own
      ;; var-ast) for the query builder's CTE generation. Left in place, that
      ;; recursively re-embeds the variable's entire state — and everything IT
      ;; wraps in turn — inside every table list that references it, one level
      ;; down from the :named-results map itself.
      (doseq [table (:selected-tables chained-blocks)]
        (assert-clean-table table))
      (doseq [[_name var-ast] (:named-results chained-blocks)
              table (concat (:tables var-ast) (:selected-tables var-ast))]
        (assert-clean-table table)))

    (testing ":group is exposed to the frontend, not pruned away entirely"
      ;; Previously absent from prune-ast's select-keys whitelist, so `ast.group`
      ;; was always undefined regardless of what the state actually computed -
      ;; the frontend (canvas mode's group-by chips, client.ts's GroupColumn)
      ;; had no way to know a `group:` clause even existed once committed.
      (is (= [{:alias "t" :column "title"}]
             (map #(select-keys % [:alias :column]) (:group single-block)))))

    (testing "an earlier variable's own entry is unaffected by how many blocks chain after it"
      ;; The actual bug wasn't about absolute size (which is arbitrary and brittle to
      ;; pin to a byte count) — it was that x's entry kept growing every time another
      ;; block chained onto it. Pruning removes the machinery (:named-results/:references/
      ;; :ast) that let that happen, so x's pruned entry here should be byte-for-byte
      ;; identical whether it's standing alone or three more blocks have chained onto
      ;; it since.
      (is (= (get-in single-block [:pending-assignments "x"])
             (get-in chained-blocks [:named-results "x"]))))))

(deftest test-api-build-doc
  (testing "the doc comment at the top of the expression comes back on the response"
    (let [response (api/api-build ["/* Tenants that never onboarded */ tenant"] nil :test)]
      (is (nil? (:error response)))
      (is (= "Tenants that never onboarded" (:doc response)))
      ;; One field, not two: there is deliberately no per-expression
      ;; :ast :doc alongside it.
      (is (nil? (get-in response [:ast :doc])))))

  (testing "no comment means no doc"
    (let [response (api/api-build ["tenant"] nil :test)]
      (is (nil? (:doc response)))))

  (testing "the tab's doc is the FIRST block's, not the last block's"
    ;; The client always sends block 0 through the active block, so the doc a
    ;; tab was opened with stays resolvable however many blocks get added
    ;; under it.
    (let [response (api/api-build ["/* Active tenants */ tenant | limit: 1 |= x"
                                   "-- second block\nx | count:"]
                                  nil :test)]
      (is (nil? (:error response)))
      (is (= "Active tenants" (:doc response)))))

  (testing "the prettified expression keeps the doc comment"
    (let [response (api/api-build ["/* Active tenants */ tenant | limit: 1"] nil :test)]
      (is (= "/* Active tenants */\ntenant\n | limit: 1"
             (get-in response [:ast :prettified]))))))

(deftest test-size-caps
  (let [long-expression (str "company | where: name = '" (apply str (repeat 70000 "a")) "'")
        post (fn [uri body]
               (api/app {:request-method :post
                         :uri uri
                         :headers {"content-type" "application/json"}
                         :body (java.io.ByteArrayInputStream. (.getBytes (json/generate-string body) "UTF-8"))}))
        body-of (fn [r] (json/parse-string (:body r) true))]
    (testing "an expression over the length cap is refused before parsing"
      (doseq [uri ["/api/v1/build" "/api/v1/eval"]]
        (is (= "too-long" (:error-type (body-of (post uri {:expressions [long-expression]}))))))
      (is (= "too-long" (:error-type (body-of (post "/api/v1/sql" {:query long-expression}))))))

    (testing "a body over 1 MB is refused from its Content-Length"
      (let [r (api/app {:request-method :post
                        :uri "/api/v1/build"
                        :headers {"content-type" "application/json"
                                  "content-length" (str (inc (* 1024 1024)))}
                        :body (java.io.ByteArrayInputStream. (.getBytes "{}" "UTF-8"))})]
        (is (= 413 (:status r)))
        (is (= "too-large" (:error-type (body-of r))))))))

(deftest test-build-reports-writes-and-keeps-hints
  (testing "/build says whether the expression changes data"
    (is (true? (:writes (api/api-build ["company | where: id = 1 | delete! .id"] nil :test))))
    (is (false? (:writes (api/api-build ["company"] nil :test)))))

  (testing "a refused write still returns the AST, so hints keep working while typing"
    (let [response (api/api-build ["company | delete! .id"] nil :test)]
      (is (nil? (:error response)))
      (is (:ast response))
      (is (true? (:writes response)))
      (is (= "write-refused" (:query-error-type response)))
      (is (re-find #"every row of `company`" (:query-error response)))
      (is (re-find #"^/\* .* \*/$" (:query response)))))

  (testing "an unresolved join still returns the AST"
    (let [response (api/api-build ["company | company"] nil :test)]
      (is (nil? (:error response)))
      (is (:ast response))
      (is (= "unresolved-join" (:query-error-type response))))))

(deftest test-eval-refuses-before-running
  ;; None of these reach the database: the :test connection has no pool.
  (testing "a write with nothing narrowing it"
    (let [response (api/api-eval ["company | delete! .id"] :test)]
      (is (= "write-refused" (:error-type response)))
      (is (re-find #"every row" (:error response)))))

  (testing "a read-only caller gets the read-only refusal, not the scope one"
    (let [response (api/api-eval ["company | delete! .id"] :test [] false)]
      (is (= "write-refused" (:error-type response)))
      (is (re-find #"read-only" (:error response)))))

  (testing "an update! with a trailing comma"
    (is (= "incomplete" (:error-type (api/api-eval ["company | where: id = 1 | u! name = 'x',"] :test))))))

(defn- request
  ([method uri] (request method uri {}))
  ([method uri headers]
   (api/app {:request-method method
             :uri uri
             :headers (merge {"content-type" "application/json"} headers)
             :body (java.io.ByteArrayInputStream. (.getBytes "{}" "UTF-8"))})))

(defmacro ^:private with-config [config & body]
  `(let [before# @api/server-config]
     (reset! api/server-config ~config)
     (try ~@body (finally (reset! api/server-config before#)))))

(deftest test-launch-token
  (with-config {:token "secret" :host "127.0.0.1"}
    (testing "without the token, every /api/ request is refused"
      (is (= 401 (:status (request :post "/api/v1/sql"))))
      (is (= 401 (:status (request :get "/api/v1/connections"))))
      (is (= 401 (:status (request :post "/api/v1/sql" {"authorization" "Bearer wrong"}))))
      (is (= 401 (:status (request :post "/api/v1/sql" {"authorization" "secret"})))))

    (testing "the 401 says why, and carries CORS headers so a browser can read it"
      (let [r (request :post "/api/v1/sql" {"origin" "https://evil.example"})]
        (is (= "unauthorized" (:error-type (json/parse-string (:body r) true))))
        (is (= "https://evil.example" (get-in r [:headers "Access-Control-Allow-Origin"])))))

    (testing "with the token, the request reaches its handler"
      (let [r (request :post "/api/v1/sql" {"authorization" "Bearer secret"})]
        (is (not= 401 (:status r)))))

    (testing "the CORS preflight isn't refused, so the real request can follow"
      (let [r (request :options "/api/v1/sql" {"origin" "http://localhost:3000"
                                               "access-control-request-method" "POST"})]
        (is (= 200 (:status r)))
        (is (get-in r [:headers "Access-Control-Allow-Origin"])))))

  (with-config {:token nil :host "127.0.0.1"}
    (testing "with no token configured, nothing is required"
      (is (not= 401 (:status (request :get "/api/v1/connections")))))))

(deftest test-host-check
  (with-config {:token nil :host "127.0.0.1"}
    (testing "a loopback-bound server refuses a request naming another host (DNS rebinding)"
      (let [r (request :get "/api/v1/connections" {"host" "evil.example:33333"})]
        (is (= 403 (:status r)))
        (is (= "forbidden" (:error-type (json/parse-string (:body r) true))))))

    (testing "loopback names, with or without a port, are fine"
      (doseq [host ["localhost:33333" "127.0.0.1:33333" "[::1]:33333" "localhost" "LOCALHOST:1"]]
        (is (not= 403 (:status (request :get "/api/v1/connections" {"host" host}))) host)))

    (testing "an IPv6 literal that isn't loopback is refused, its colons not read as a port"
      (is (= 403 (:status (request :get "/api/v1/connections" {"host" "[::2]:33333"}))))))

  (with-config {:token nil :host "0.0.0.0"}
    (testing "a server bound to every interface skips the check"
      (is (not= 403 (:status (request :get "/api/v1/connections" {"host" "pine.example"})))))))

(deftest test-json-params-only
  ;; A form POST is a CORS "simple request": a web page can send one without a
  ;; preflight. The route must not read parameters from it, or from the query
  ;; string.
  (let [seen (atom ::not-called)]
    (with-redefs [api/api-sql (fn [query _] (reset! seen query) {})
                  pine.db.main/connection-id (atom :test)]
      (testing "a form-encoded body is not read as parameters"
        (api/app {:request-method :post
                  :uri "/api/v1/sql"
                  :headers {"content-type" "application/x-www-form-urlencoded"}
                  :body (java.io.ByteArrayInputStream. (.getBytes "query=select+1" "UTF-8"))})
        (is (nil? @seen)))

      (testing "nor is the query string"
        (reset! seen ::not-called)
        (api/app {:request-method :post
                  :uri "/api/v1/sql"
                  :query-string "query=select+1"
                  :headers {"content-type" "application/json"}
                  :body (java.io.ByteArrayInputStream. (.getBytes "{}" "UTF-8"))})
        (is (nil? @seen)))

      (testing "a JSON body is"
        (reset! seen ::not-called)
        (api/app {:request-method :post
                  :uri "/api/v1/sql"
                  :headers {"content-type" "application/json"}
                  :body (java.io.ByteArrayInputStream. (.getBytes "{\"query\": \"select 1\"}" "UTF-8"))})
        (is (= "select 1" @seen))))))

(defn- post-json [uri body]
  (let [r (api/app {:request-method :post
                    :uri uri
                    :headers {"content-type" "application/json"}
                    :body (java.io.ByteArrayInputStream. (.getBytes (json/generate-string body) "UTF-8"))})]
    (assoc r :json (try (json/parse-string (:body r) true) (catch Exception _ nil)))))

(deftest test-ordinary-mistakes-are-not-500s
  (with-redefs [pine.db.main/connection-id (atom nil)]
    (testing "a parameter of the wrong type is a 400 naming it"
      (let [r (post-json "/api/v1/sql" {:query 5})]
        (is (= 400 (:status r)))
        (is (= "bad-request" (get-in r [:json :error-type]))))
      (is (= 400 (:status (post-json "/api/v1/build" {:expressions 5}))))
      (is (= 400 (:status (post-json "/api/v1/eval" {:expressions [1 2]}))))
      (is (= 400 (:status (post-json "/api/v1/build" {:expressions ["company"] :connection-id 7}))))
      (is (= 400 (:status (post-json "/api/v1/eval" {:expressions ["company"] :variables [1]})))))

    (testing "no connection selected is a normal error"
      (doseq [[uri body] [["/api/v1/build" {:expressions ["company"]}]
                          ["/api/v1/eval" {:expressions ["company"]}]
                          ["/api/v1/sql" {:query "select 1"}]]]
        (let [r (post-json uri body)]
          (is (= 200 (:status r)) uri)
          (is (= "no-connection" (get-in r [:json :error-type])) uri)))
      (let [r (api/app {:request-method :get :uri "/api/v1/connection/stats" :headers {}})]
        (is (= 200 (:status r)))))

    (testing "an unknown connection id is a normal error"
      (let [r (post-json "/api/v1/build" {:expressions ["company"] :connection-id "nope"})]
        (is (= 200 (:status r)))
        (is (= "no-connection" (get-in r [:json :error-type])))))

    (testing "the legacy route without an expression doesn't throw"
      (is (= 200 (:status (post-json "/api/v1/build-with-params" {})))))))

(deftest test-internal-errors-dont-leak
  (with-redefs [api/api-sql (fn [& _] (throw (NullPointerException. "secret internals")))
                pine.db.main/connection-id (atom :test)]
    (let [r (post-json "/api/v1/sql" {:query "select 1"})]
      (is (= 500 (:status r)))
      (is (= "internal" (get-in r [:json :error-type])))
      (is (not (re-find #"secret" (:body r)))))))

(deftest test-build-keeps-hints-when-the-policy-refuses
  ;; MCP's complete_query builds under the connection's policy. A refused
  ;; query (here, a table missing from the schema index) must still return
  ;; the AST and hints, with the refusal in query-error.
  (let [response (api/api-build ["secrets"] nil :test [{:type "column-type" :allow ["integer"]}])]
    (is (nil? (:error response)))
    (is (:ast response))
    (is (= "policy" (:query-error-type response)))))

(deftest test-cancel-route
  (testing "a run id is required, and must be a string"
    (is (= 400 (:status (post-json "/api/v1/cancel" {}))))
    (is (= 400 (:status (post-json "/api/v1/cancel" {:run-id 5}))))
    (is (= 400 (:status (post-json "/api/v1/sql" {:query "select 1" :run-id ""})))))
  (testing "stopping a run that isn't running is not an error"
    (let [r (post-json "/api/v1/cancel" {:run-id "api-test-not-running"})]
      (is (= 200 (:status r)))
      (is (= {:running false} (:json r))))
    (pine.db.exec/finish-run! "api-test-not-running")))
