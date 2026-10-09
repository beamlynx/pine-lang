(ns pine.api
  "HTTP API layer: routes, request parsing, and multi-expression evaluation.

  Why multi-expression evaluation exists: Pine expressions are designed to be
  composed across blank-line-separated blocks. Each block can assign its result
  to a named result (|= name) which subsequent blocks use as a CTE. This file
  threads named results between expressions so each one sees what earlier ones
  produced. The last expression's SQL is the one actually returned or executed."
  (:require
   [cheshire.core :as json]
   [cheshire.generate :refer [add-encoder encode-str]]
   [clojure.string :as str]
   [compojure.core :refer [defroutes DELETE GET POST]]
   [compojure.route :as route]
   [pine.access-policy :as access-policy]
   [pine.ast.effects :as effects]
   [pine.ast.main :as ast]
   [pine.db.connections :as connections] ;; Encode arrays and json results in API responses
   [pine.db.exec :as exec]
   [pine.db.main :as db]
   [pine.eval :as eval]
   [pine.parser :as parser]
   [pine.variables :as variables]
   [pine.version :as v]
   [ring.middleware.cors :refer [wrap-cors]]
   [ring.middleware.defaults :refer [api-defaults wrap-defaults]]
   [ring.middleware.json :refer [wrap-json-params wrap-json-response]]
   [ring.util.response :refer [response]])
  (:import
   [java.util TimeZone]))

;; Set default timezone to UTC
(TimeZone/setDefault (TimeZone/getTimeZone "UTC"))

;; array/json encoding
(add-encoder org.postgresql.util.PGobject encode-str)
(add-encoder org.postgresql.jdbc.PgArray encode-str)

;; MySQL Connector/J returns java.time (JSR-310) values for DATE/DATETIME/
;; TIME columns rather than the java.sql.Date/Timestamp Postgres's driver
;; returns (which Cheshire already knows how to encode) - without these,
;; any query result touching one of those columns throws
;; JsonGenerationException while writing the HTTP response, which escapes
;; past wrap-cors (the response never gets its headers) and reaches the
;; browser as an unhelpful bare "blocked by CORS policy" / 500 with no
;; body. .toString() alone is enough: all three already render ISO-8601.
(add-encoder java.time.LocalDateTime encode-str)
(add-encoder java.time.LocalDate encode-str)
(add-encoder java.time.LocalTime encode-str)

(def version v/version)

(defn- log-exception
  "Every catch below turns a failure into a normal {:error ...} response -
  correct, a route handler throwing shouldn't 500 just because the
  underlying operation failed (a bad connection, an unreachable database).
  But none of them printed anything before this, which made a confusing
  message (or a message that's merely the symptom, e.g. a MySQL connection
  silently defaulting to Postgres and failing during SSL negotiation)
  effectively undebuggable without reproducing it by hand outside the
  server. `context` is a short label (e.g. \"connect\", \"api-eval\") naming
  which operation failed, since the caught exception's own message alone
  doesn't always say that."
  [context ^Throwable e]
  (prn (format "[%s] %s: %s" context (.getName (class e)) (.getMessage e))))

;; An exception's message and, when it carries one, its :error-type
;; (write-refused, unresolved-join, ...), so a client can tell kinds apart.
(defn- error-body [^Throwable e]
  (cond-> {:error (or (.getMessage e) (str "Failed (" (.getSimpleName (class e)) ")."))}
    (:error-type (ex-data e)) (assoc :error-type (:error-type (ex-data e)))))

(defn- build-sql
  "The SQL preview for /build. Building it can fail where the AST didn't: an
  unresolved join, or a delete!/update! that would be refused. The AST and
  hints are still worth returning then - the person is usually mid-way
  through typing - so the failure goes in :query-error instead of failing
  the whole build, and :query says why as an SQL comment."
  [state]
  (try
    {:query (-> state eval/build-query eval/formatted-query)}
    (catch Exception e
      (when-not (instance? clojure.lang.ExceptionInfo e)
        (log-exception "api-build query" e))
      (let [{:keys [error error-type]} (error-body e)]
        (cond-> {:query (str "/* " (str/replace error "*/" "* /") " */")
                 :query-error error}
          error-type (assoc :query-error-type error-type))))))

(defn- generate-state
  ([expression]
   (generate-state expression nil nil {}))
  ([expression cursor]
   (generate-state expression cursor nil {}))
  ([expression cursor connection-id]
   (generate-state expression cursor connection-id {}))
  ([expression cursor connection-id named-results]
   (generate-state expression cursor connection-id named-results []))
  ([expression cursor connection-id named-results access-policy]
   (let [{:keys [result error]} (->> expression parser/parse)
         conn-id (or connection-id @db/connection-id)]
     (if result
       ;; `$name`s with a value in this request become ordinary literals here,
       ;; before the AST sees them (pine.variables).
       {:result (ast/generate (variables/bind result) conn-id expression cursor named-results access-policy)}
       {:error-type "parse"
        :error error}))))

(defn- evaluate-expressions
  "Evaluate a sequence of pine expressions, threading named results from |= assignments
  into subsequent expressions. Returns {:last-state <state> :error <msg>}.

  Why pending-assignments: |= is now a mid-pipeline op that snapshots state at the
  point of assignment. An expression can assign several named results before its last op
  determines the final SQL. All assignments from one expression become available as
  CTEs to the next."
  ([expressions connection-id]
   (evaluate-expressions expressions connection-id []))
  ([expressions connection-id access-policy]
   (reduce (fn [{:keys [named-results]} expression]
             (let [{:keys [result error]} (generate-state expression nil connection-id named-results access-policy)]
               (if error
                 (reduced {:error error})
                 {:named-results (merge named-results (:pending-assignments result))
                  :last-state result})))
           {:named-results {} :last-state nil}
           expressions)))

(defn- trim-pipes [s]
  (-> s
      (str/trim)
      (str/replace #"^\|\s*|\s*\|$" "")
      (str/trim)))

(defn- prune-table
  "Prune a table entry down to what the frontend's Table type (client.ts) uses.
  Critical: a variable-backed table entry carries a full :ast (the variable's own
  var-ast) for the query builder's CTE generation — left unpruned, it recursively
  re-embeds that variable's entire state (and, transitively, everything it wraps in
  turn) inside every table list that references it."
  [table]
  (select-keys table [:schema :table :alias]))

(defn- prune-var-ast
  "Prune a variable/pending-assignment snapshot down to what the frontend actually
  uses (NamedResultAst in client.ts). Critical: a raw snapshot still carries :named-results
  and :references from pre-handle/post-handle — left unpruned, each additional
  chained |= block would re-embed every earlier block's full snapshot inside the new
  one, growing the response payload superlinearly instead of linearly. Its own
  :tables/:selected-tables entries need the same per-table pruning (prune-table) for
  a variable-of-variable chain, or the same recursive embedding reappears one level
  down."
  [var-ast]
  (-> (select-keys var-ast [:tables :selected-tables :joins :columns])
      (update :tables #(mapv prune-table %))
      (update :selected-tables #(mapv prune-table %))))

(defn- prune-ast
  "Prune a generated state down to the :ast value returned to the frontend."
  [state]
  (-> (select-keys state [:hints :selected-tables :joins :context :current :operation :columns :order :where :group :prettified :ranges :assign])
      (update :selected-tables #(mapv prune-table %))
      (assoc :named-results
             (into {} (for [[k v] (:named-results state)] [k (prune-var-ast v)])))
      (assoc :pending-assignments
             (into {} (for [[k v] (:pending-assignments state)] [k (prune-var-ast v)])))))

(defn api-build
  ([expressions]
   (api-build expressions nil nil))
  ([expressions cursor]
   (api-build expressions cursor nil))
  ([expressions cursor connection-id]
   (api-build expressions cursor connection-id []))
  ([expressions cursor connection-id access-policy]
   (let [conn-id (or connection-id @db/connection-id)
         connection-name (connections/get-connection-name conn-id)]
     (try
       (let [exprs         (if (string? expressions) [expressions] expressions)
             context-exprs (butlast exprs)
             ;; nil (missing/absent last expression) is the only value the
             ;; parser can't handle - "" and blank strings parse fine into an
             ;; empty :table op, which is what lets an empty input still show
             ;; table hints on Tab instead of "nothing found".
             last-expr     (or (last exprs) "")]
         (let [{:keys [named-results error]} (evaluate-expressions context-exprs conn-id access-policy)]
           (if error
             {:connection-id connection-name :error error}
             ;; The AST describes the expression as written, so a $variable
             ;; stays `$name` in it (the canvas shows it that way); only :query,
             ;; the SQL preview below, gets the values.
             (let [result                    (binding [variables/*bindings* {}]
                                               (generate-state last-expr cursor conn-id named-results access-policy))
                   {state :result build-error :error} result]
               (if build-error
                 {:connection-id connection-name :error build-error}
                 (merge
                  {:connection-id connection-name
                   :version version
                  ;; Whether the expression changes data, as /eval reports it.
                  ;; Lets a caller refuse a write before running anything.
                   :writes (effects/any-writes? (:operation-types state))
                  ;; The doc comment for this tab, which lives at the top of
                  ;; the FIRST expression. Deliberately not also exposed per
                  ;; expression on :ast - a tab has one description, and a
                  ;; second field of the same name meaning "the last
                  ;; expression's" was only ever confusing. The client always
                  ;; sends block 0, so this is always resolvable.
                   :doc (some-> exprs first parser/extract-doc :text)
                  ;; Every $variable the expressions use, and those with no
                  ;; value in this request. Never an error here: a template
                  ;; keeps its hints. /eval is the one that refuses.
                   :variables (variables/report exprs variables/*bindings*)
                   :ast (prune-ast state)}
                  (-> last-expr trim-pipes (generate-state nil conn-id named-results access-policy) :result build-sql)))))))
       (catch Exception e
         (log-exception "api-build" e)
         {:connection-id connection-name
          ;; Never a null error: some exceptions (a NullPointerException)
          ;; have no message, and a client reads {:error nil} as success.
          :error (or (.getMessage e) (str "Couldn't build this expression (" (.getSimpleName (class e)) ")."))})))))

(defn- get-columns
  ([rows]
   (if (seq rows)
     (mapv (fn [col] {:column col}) (first rows))
     []))
  ([state rows]
   (let [state-columns (-> state :columns)
         row-columns (if (seq rows)
                       (-> rows first)
                       [])
         remaining-columns (->> row-columns
                                (drop (count state-columns))
                                (map (fn [col] {:column col :alias (-> state :current)})))]
     (concat state-columns
             remaining-columns
             (when-let [alias (state :alias)]
               [alias])))))

(defn api-eval
  ([expressions]
   (api-eval expressions nil))
  ([expressions connection-id]
   (api-eval expressions connection-id []))
  ([expressions connection-id access-policy]
   (api-eval expressions connection-id access-policy true))
  ;; allow-writes false refuses to execute an expression that changes data,
  ;; before it runs. Absent (and so true) for every existing caller: the
  ;; human's own UI runs delete!/update! on purpose. The MCP relay is the one
  ;; caller that passes false, unconditionally.
  ([expressions connection-id access-policy allow-writes]
   (let [conn-id (or connection-id @db/connection-id)
         connection-name (connections/get-connection-name conn-id)]
     (try
       (let [exprs         (if (string? expressions) [expressions] expressions)
             context-exprs (butlast exprs)
             last-expr     (last exprs)
             trimmed       (trim-pipes (or last-expr ""))]
         (if (str/blank? trimmed)
           {:connection-id connection-name}
           (let [{:keys [named-results error]} (evaluate-expressions context-exprs conn-id access-policy)]
             (if error
               {:connection-id connection-name :error error}
               (let [{last-state :result build-error :error} (generate-state trimmed nil conn-id named-results access-policy)]
                 (if build-error
                   {:connection-id connection-name :error build-error}
                   (let [writes (effects/any-writes? (:operation-types last-state))]
                     (if (and (false? allow-writes) writes)
                       {:connection-id connection-name
                        :error-type "write-refused"
                        :error (str "Refusing to run an expression that changes data: "
                                    (str/join ", " (map name (filter effects/writes?
                                                                     (:operation-types last-state))))
                                    ". This caller asked for read-only evaluation.")
                        :writes true}
                       (try
                         (let [rows    (eval/run-query last-state)
                               op-type (get-in last-state [:operation :type])
                               columns (if (effects/writes? op-type)
                                         (get-columns rows)
                                         (get-columns last-state rows))]
                           {:connection-id connection-name
                            :version version
                            :result rows
                            :columns columns
                            ;; Whether this expression changes data. Reported on
                            ;; every eval, not only a refused one, so a caller
                            ;; can tell what it just ran.
                            :writes writes
                            ;; Already computed as part of generate-state's shared
                            ;; post-handle pipeline (ast/main.clj's add-prettify runs
                            ;; on every build or eval alike) - free to expose here
                            ;; without a second /api/v1/build round trip, unlike
                            ;; client.ts's prettify() which pays for one on purpose.
                            :prettified (:prettified last-state)})
                         (catch Exception e
                           (log-exception "api-eval" e)
                           (merge {:connection-id connection-name
                                   ;; nil when the SQL itself couldn't be built
                                   ;; (a refused write, an unresolved join).
                                   :query (try (-> last-state eval/build-query eval/formatted-query)
                                               (catch Exception _ nil))
                                   :writes writes
                                   :prettified (:prettified last-state)}
                                  (error-body e))))))))))))
       (catch Exception e
         (log-exception "api-eval" e)
         (assoc (error-body e) :connection-id connection-name))))))

(defn get-connection []
  (let [connection-id   @db/connection-id]
    (if connection-id
      (let [connection-name (connections/get-connection-name connection-id)
            _               (db/init-references @db/connection-id)]
        {:result
         {:connection-id connection-name
          :version version}})
      {:result
       {:connection-id ""
        :version version}})))

(defn get-connections []
  {:result
   {:version version
    :selected-connection-id @db/connection-id
    :connections (connections/list-connections)}})

(defn test-connection [id]
  (let [result (db/run-query id {:query "SELECT CURRENT_TIMESTAMP;"})]
    {:connection-id id :time result}))

(defn set-connection-pool [id]
  {:version version
   :connection-id (db/set-connection id)})

(defn create-connection [connection]
  (try
    {:connection-id (connections/add-connection-pool connection)}
    (catch Exception e
      (log-exception "create-connection" e)
      {:error (.getMessage e)})))

(defn connect [id]
  (try
    (-> id test-connection :connection-id set-connection-pool)
    (catch Exception e
      (log-exception "connect" e)
      {:error (.getMessage e)})))

(defn disconnect [id]
  (try
    (connections/remove-connection-pool id)
    (db/clear-connection-if id)
    (get-connections)
    (catch Exception e
      (log-exception "disconnect" e)
      {:error (.getMessage e)})))

(defn reindex-connection [id]
  (try
    {:connection-id (db/reindex-references id)}
    (catch Exception e
      (log-exception "reindex-connection" e)
      {:error (.getMessage e)})))

(defn api-sql
  ([sql-query]
   (api-sql sql-query nil))
  ([sql-query connection-id]
   (let [conn-id (or connection-id @db/connection-id)
         connection-name (connections/get-connection-name conn-id)]
     (cond
       (nil? sql-query)
       {:connection-id connection-name
        :error "SQL query is required. Please provide a 'query' parameter in the request body."}

       (clojure.string/blank? sql-query)
       {:connection-id connection-name
        :error "SQL query cannot be empty."}

       :else
       (try
         (let [result (db/run-sql conn-id sql-query)
               columns (when (vector? result) (get-columns result))]
           {:connection-id connection-name
            :version version
            :result result
            :columns columns})
         (catch Exception e
           (log-exception "api-sql" e)
           (assoc (error-body e) :connection-id connection-name)))))))

(defn wrap-logger
  [handler]
  (fn [request]
    (let [response (handler request)]
      (when (= 404 (:status response))
        (prn (format "Path not found: %s" (:uri request))))
      response)))

(defn wrap-exception-logging
  "Every route handler below already has its own try/catch that turns a
  failure into a normal {:error ...} response (api-eval, api-sql, connect,
  etc.), but none of them print anything - an unhelpful exception (a bare
  NullPointerException, say) left nothing to go on server-side, only
  whatever string reached the client.

  Wrapping wrap-json-response (not just app-routes) matters just as much:
  an exception thrown *while encoding* an otherwise-successful response
  (e.g. a java.time.LocalDateTime value - MySQL's JDBC driver returns one
  for DATETIME/TIMESTAMP columns - that Cheshire has no encoder for) used
  to happen entirely outside any try/catch here, so it reached Jetty as a
  bare, silent 500 that skipped wrap-cors's response headers too. Skipping
  those headers is what actually confuses a browser: its only way to
  describe 'this response had no CORS header' is a generic 'blocked by
  CORS policy' message, which looks like a CORS misconfiguration even
  though CORS was never the problem. Catching everything here and
  returning a normal, already-JSON-encoded response guarantees wrap-cors
  (which wraps this) still gets a chance to add its headers."
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch Throwable t
        (prn (format "Unhandled exception on %s %s: %s"
                     (-> request :request-method name str/upper-case)
                     (:uri request)
                     t))
        (.printStackTrace t)
        ;; The real exception went to the log above. Its message can carry
        ;; internals (class names, SQL, file paths) the caller has no use for.
        {:status 500
         :headers {"Content-Type" "application/json"}
         :body (json/generate-string {:error-type "internal" :error "Internal error. See the server log."})}))))

;; The request's `$variable` values, checked and bound for the duration of f
;; (see pine.variables): those written in its values blocks, overridden by
;; any passed in its `variables` param. f gets the expressions with the values
;; blocks taken out, which is all the rest of Pine ever sees. The values
;; written in the text are added to /build's report. A malformed values block
;; or `variables` map is reported, not thrown.
(defn- usable-cursor
  "The cursor, or nil when it can't point into the expression: a cursor
  before its first line or column means the client measured it from another
  block, and is ignored rather than failing the build."
  [cursor]
  (when (and (map? cursor) (nat-int? (:line cursor)) (nat-int? (:character cursor)))
    cursor))

(defn- tab-doc
  "The tab's doc comment: the first one in its leading values blocks or its
  first query block. A tab often starts with its values, with the doc on
  the query below them."
  [exprs]
  (->> exprs
       (reduce (fn [acc e] (if (variables/values-block? e) (conj acc e) (reduced (conj acc e)))) [])
       (some #(some-> % parser/extract-doc :text))))

(defn- with-variables [params exprs f]
  (try
    (let [written (variables/text-values exprs)]
      (binding [variables/*bindings* (merge written (variables/normalize (:variables params)))]
        (let [response (f (vec (remove variables/values-block? exprs)))]
          (cond-> response
            (:variables response) (assoc-in [:variables :values] written)))))
    (catch clojure.lang.ExceptionInfo e
      (if (= "variables" (:error-type (ex-data e)))
        {:error-type "variables" :error (.getMessage e)}
        (throw e)))))

;; Longer than any expression a person writes. Checked before parsing, so a
;; pasted file or a hostile request can't make the parser do unbounded work.
(def max-expression-length 65536)

(defn- too-long-response
  "The error for an expression or query over max-expression-length, or nil
  when every one of `texts` (a string, a sequence of them, or nil) is within
  it. Non-strings are left for the route's own handling."
  [texts]
  (when (some #(and (string? %) (> (count %) max-expression-length))
              (if (sequential? texts) texts [texts]))
    {:error-type "too-long"
     :error (str "Expression is longer than " max-expression-length " characters.")}))

;; Bigger than any request beamlynx sends. Checked from Content-Length before
;; anything reads the body.
(def max-body-bytes (* 1024 1024))

(defn wrap-body-limit [handler]
  (fn [request]
    (let [length (some-> (get-in request [:headers "content-length"]) parse-long)]
      (if (and length (> length max-body-bytes))
        ;; Encoded by hand: this sits outside wrap-json-response.
        {:status 413
         :headers {"Content-Type" "application/json"}
         :body (json/generate-string {:error-type "too-large"
                                      :error "Request body larger than 1 MB."})}
        (handler request)))))

;; ---------------------------------------------------------------------------
;; Who may call this server
;; ---------------------------------------------------------------------------
;;
;; The server binds loopback by default (core.clj), but that alone doesn't
;; keep other callers out. Any web page in the person's browser can send it
;; requests, and every program on the machine can. Two checks close that:
;;
;; - The launch token. beamlynx-desktop makes a random one each launch and
;;   passes it as PINE_TOKEN; its UI sends it as `Authorization: Bearer`.
;;   With a token set, every /api/ request without it is refused.
;; - The Host header. When bound to loopback, a request must name a loopback
;;   host. A web page that rebinds its own domain to 127.0.0.1 (DNS
;;   rebinding) sends its own domain here, so it is refused even without a
;;   token.

(defonce ^{:doc "The launch token and bind host. Read from the environment;
  core.clj sets it again from what it actually binds, and tests reset! it."}
  server-config
  (atom {:token (not-empty (System/getenv "PINE_TOKEN"))
         :host  (or (System/getenv "PINE_HOST") "127.0.0.1")}))

(defn- json-error [status error-type message]
  {:status status
   :headers {"Content-Type" "application/json"}
   :body (json/generate-string {:error-type error-type :error message})})

(defn- token-matches? [expected header]
  (and (string? header)
       (str/starts-with? header "Bearer ")
       ;; Constant time, so the comparison doesn't leak how much matched.
       (java.security.MessageDigest/isEqual
        (.getBytes ^String expected "UTF-8")
        (.getBytes ^String (subs header 7) "UTF-8"))))

(defn wrap-auth
  "With a launch token configured, refuse every /api/ request that doesn't
  carry it. OPTIONS is let through so the browser's CORS preflight works; the
  real request after it is checked."
  [handler]
  (fn [request]
    (let [token (:token @server-config)]
      (if (and token
               (not= :options (:request-method request))
               (str/starts-with? (or (:uri request) "") "/api/")
               (not (token-matches? token (get-in request [:headers "authorization"]))))
        (json-error 401 "unauthorized" "This server requires the launch token.")
        (handler request)))))

(defn- loopback-bind? [host]
  (boolean (or (#{"localhost" "::1" "[::1]"} host)
               (str/starts-with? (or host "") "127."))))

(defn- host-name
  "The host part of a Host header: `localhost:33333` -> `localhost`,
  `[::1]:33333` -> `[::1]`. An IPv6 literal keeps its brackets, and its own
  colons are not mistaken for a port."
  [header]
  (if (str/starts-with? header "[")
    (subs header 0 (inc (or (str/index-of header "]") (dec (count header)))))
    (first (str/split header #":" 2))))

(def ^:private loopback-host-names #{"localhost" "127.0.0.1" "[::1]"})

(defn wrap-host-check
  "When bound to loopback, refuse a request whose Host header names anything
  else. A request with no Host header (HTTP/1.0) is let through."
  [handler]
  (fn [request]
    (let [header (get-in request [:headers "host"])]
      (if (and header
               (loopback-bind? (:host @server-config))
               (not (loopback-host-names (str/lower-case (host-name header)))))
        (json-error 403 "forbidden" "Host header not allowed.")
        (handler request)))))

;; ---------------------------------------------------------------------------
;; Request checks
;; ---------------------------------------------------------------------------
;;
;; Run by the routes before anything else, so an ordinary mistake (no
;; connection selected, a parameter of the wrong type) gets a normal error
;; instead of an exception surfacing as HTTP 500.

(defn- bad-request [message]
  {:status 400
   :headers {"Content-Type" "application/json"}
   :body {:error-type "bad-request" :error message}})

(defn- param-problem
  "A 400 response for the first parameter of the wrong type, or nil."
  [{:keys [expressions expression connection-id variables query run-id]} & {:keys [sql?]}]
  (cond
    (and (some? expressions) (not (and (sequential? expressions) (every? string? expressions))))
    (bad-request "`expressions` must be a list of strings.")

    (and (some? expression) (not (string? expression)))
    (bad-request "`expression` must be a string.")

    ;; JSON only produces strings; the keyword sentinels are the tests'
    ;; fixture connections (pine.db.connections/test-connection-ids).
    (and (some? connection-id) (not (string? connection-id))
         (not (connections/test-connection? connection-id)))
    (bad-request "`connection-id` must be a string.")

    (and (some? variables) (not (map? variables)))
    (bad-request "`variables` must be an object.")

    (and sql? (some? query) (not (string? query)))
    (bad-request "`query` must be a string.")

    (and (some? run-id) (not (and (string? run-id) (<= 1 (count run-id) 100))))
    (bad-request "`run-id` must be a string of 1 to 100 characters.")))

(defn- with-run-id
  "Runs (f) with the request's run id bound, so /cancel can stop the
  statements it starts. A request without one can't be stopped."
  [run-id f]
  (if run-id
    (binding [exec/*run-id* run-id]
      (try (f) (finally (exec/finish-run! run-id))))
    (f)))

(defn- connection-problem
  "An error response when there is no connection to use, or nil."
  [connection-id]
  (let [conn-id (or connection-id @db/connection-id)]
    (cond
      (nil? conn-id)
      {:error-type "no-connection" :error "No connection selected. Connect to a database first."}

      (not (try (connections/get-connection-name conn-id) true
                (catch clojure.lang.ExceptionInfo _ false)))
      {:error-type "no-connection" :error (str "Connection `" conn-id "` isn't connected. Connect to it again.")})))

;; TODO: POST method should return 401

(defroutes app-routes
  ;; connection management
  (GET "/api/v1/connection" [] (-> (get-connection) response))
  (GET "/api/v1/connections" [] (-> (get-connections) response))
  (POST "/api/v1/connections" req
    (let [connection (get-in req [:params])]
      (-> connection create-connection response)))
  (POST "/api/v1/connections/:id/connect" [id]
    (-> id connect response))
  (POST "/api/v1/connections/:id/reindex" [id]
    (-> id reindex-connection response))
  (DELETE "/api/v1/connections/:id" [id]
    (-> id disconnect response))
  (GET "/api/v1/connection/stats" []
    (response
     (or (connection-problem nil)
         {:connection-count (db/get-connection-count @db/connection-id)
          :version version
          :time (str (java.time.LocalDateTime/now))})))

  ;; query building and evaluation
  (POST "/api/v1/build" {params :params}
    (let [{:keys [expressions expression cursor connection-id]} params
          exprs (or expressions (when expression [expression]))
          rules (access-policy/sanitize-rules (:access-policy params))]
      (or
       (param-problem params)
       (response
        (or
         (too-long-response exprs)
         (connection-problem connection-id)
         (let [built (with-variables params exprs
                       #(api-build % (usable-cursor cursor) connection-id rules))]
          ;; api-build only sees the blocks left after values blocks are taken
          ;; out, so the doc comes from the full list here.
           (cond-> built
             (contains? built :doc) (assoc :doc (tab-doc exprs)))))))))
  (POST "/api/v1/eval" {params :params}
    (let [{:keys [expressions expression connection-id]} params
          exprs (or expressions (when expression [expression]))
          rules (access-policy/sanitize-rules (:access-policy params))
          ;; Only an explicit false turns writes off - a missing or malformed
          ;; value must never read as "allowed to write" by accident, and must
          ;; never read as "refuse everything" for the callers that don't send it.
          allow-writes (not (false? (:allow-writes params)))]
      (or
       (param-problem params)
       (response
        (or
         (too-long-response exprs)
         (connection-problem connection-id)
         (with-variables params exprs
           (fn [query-exprs]
           ;; Every $variable must have a value before anything runs.
             (if-let [unbound (seq (:unbound (variables/report query-exprs variables/*bindings*)))]
               {:error-type "unbound-variable"
                :error (variables/missing-message unbound)
                :unbound (vec unbound)}
               (with-run-id (:run-id params)
                 #(api-eval query-exprs connection-id rules allow-writes))))))))))

  ;; raw SQL execution
  (POST "/api/v1/sql" {params :params}
    (let [{:keys [query connection-id]} params]
      (or (param-problem params :sql? true)
          (response (or (too-long-response query)
                        (connection-problem connection-id)
                        (with-run-id (:run-id params)
                          #(api-sql query connection-id)))))))

  ;; Stop a run started with this `run-id` on /eval or /sql. The run
  ;; answers with `error-type: "cancelled"`; a write in progress is rolled
  ;; back. Stopping a run that has finished, or hasn't started, is not an
  ;; error: `running` says whether a statement was stopped.
  (POST "/api/v1/cancel" {params :params}
    (let [{:keys [run-id]} params]
      (or (param-problem params)
          (if (nil? run-id)
            (bad-request "`run-id` is required.")
            (response {:running (exec/cancel! run-id)})))))

  ;; Legacy
  ;;
  ;; pine-mode.el
  (POST "/api/v1/build-with-params" {params :params}
    (let [{:keys [expression connection-id]} params]
      (or (param-problem params)
          (some-> (connection-problem connection-id) response)
          (->> (api-build [(trim-pipes (or expression ""))] nil connection-id) :query response))))
  ;; default case
  (route/not-found "Not Found"))
(def app
  (-> app-routes
      (wrap-json-params {:keywords? true})
      wrap-json-response
      wrap-exception-logging
      wrap-logger
;; Parameters come from the JSON body only. api-defaults also reads
      ;; form-encoded bodies and the query string; a form POST is a CORS
      ;; "simple request" that a web page can send without a preflight.
      (wrap-defaults (assoc-in api-defaults [:params :urlencoded] false))
      wrap-body-limit
      wrap-auth
      wrap-host-check
      (wrap-cors :access-control-allow-origin [#".*"]
                 :access-control-allow-methods [:get :post :put :delete])))
