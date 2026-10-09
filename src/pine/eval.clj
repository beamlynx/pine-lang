(ns pine.eval
  (:require
   [cheshire.core :as json]
   [clojure.string :as s]
   [pine.access-policy :as access-policy]
   [pine.data-types :as dt]
   [pine.db.connections :as connections]
   [pine.db.main :as db]))

(def ^:dynamic *dialect*
  "Which SQL dialect build-query is rendering for - bound once at the top of
  build-query from the state's :connection-id, then read by every pure
  string-builder below it in the same call tree (q, casts, date bucketing,
  the action-subquery wrap). Defaults to :postgres so eval/q and friends
  stay usable standalone, outside any build-query call, exactly as before
  dialects existed."
  :postgres)

(defn q
  ([a b]
   (if a (str (q a) "." (q b)) (q b)))
  ([a]
   ;; A quote inside the name is written twice. A JSON path column is named
   ;; after its keys, and a key can hold any character.
   (let [quote-char (if (= *dialect* :mysql) "`" "\"")]
     (str quote-char (s/replace (str a) quote-char (str quote-char quote-char)) quote-char))))

(defn- col-fn-format
  "Map column function names to TO_CHAR format strings"
  [col-fn]
  (case col-fn
    "year"   "YYYY"
    "month"  "YYYY-MM"
    "day"    "YYYY-MM-DD"
    "week"   "YYYY-MM-DD"
    "hour"   "YYYY-MM-DD HH24"
    "minute" "YYYY-MM-DD HH24:MI"))

(defn- col-fn-expr
  "Render a date-bucketing expression for col-fn (e.g. `select: created_at =>
  month`). MySQL's shape isn't just a different format string like
  Postgres's TO_CHAR(DATE_TRUNC(...)) - week in particular needs its own
  DATE_SUB/WEEKDAY expression, since MySQL has no DATE_TRUNC. WEEKDAY()
  returns 0 for Monday, matching Postgres's DATE_TRUNC('week') boundary."
  [col-fn col-ref]
  (case *dialect*
    :mysql (case col-fn
             "year"   (str "DATE_FORMAT(" col-ref ", '%Y')")
             "month"  (str "DATE_FORMAT(" col-ref ", '%Y-%m')")
             "day"    (str "DATE_FORMAT(" col-ref ", '%Y-%m-%d')")
             "week"   (str "DATE_FORMAT(DATE_SUB(" col-ref ", INTERVAL WEEKDAY(" col-ref ") DAY), '%Y-%m-%d')")
             "hour"   (str "DATE_FORMAT(" col-ref ", '%Y-%m-%d %H')")
             "minute" (str "DATE_FORMAT(" col-ref ", '%Y-%m-%d %H:%i')"))
    ;; SQLite has no date type: a date is text (`2024-01-31 10:00:00`), which
    ;; strftime reads. %w counts Sunday as 0, so (%w + 6) % 7 is the days
    ;; since Monday - the boundary Postgres's DATE_TRUNC('week') uses.
    :sqlite (case col-fn
              "year"   (str "strftime('%Y', " col-ref ")")
              "month"  (str "strftime('%Y-%m', " col-ref ")")
              "day"    (str "strftime('%Y-%m-%d', " col-ref ")")
              "week"   (str "strftime('%Y-%m-%d', " col-ref ", '-' || ((CAST(strftime('%w', " col-ref ") AS INTEGER) + 6) % 7) || ' days')")
              "hour"   (str "strftime('%Y-%m-%d %H', " col-ref ")")
              "minute" (str "strftime('%Y-%m-%d %H:%M', " col-ref ")"))
    (str "TO_CHAR(DATE_TRUNC('" col-fn "', " col-ref "), '" (col-fn-format col-fn) "')")))

(def ^:private mysql-cast-types
  "Translation table for a user's explicit `::cast` (and the internal
  ::text cast used to reconcile a mismatched heuristic join) into MySQL's
  CAST() target vocabulary, which is closed - CAST(x AS TEXT) is a hard
  syntax error, unlike Postgres's x::text. Anything not listed here is
  uppercased and passed through, same \"trust the user\" behavior Postgres
  already has for casts it doesn't specifically know about."
  {"text" "CHAR" "varchar" "CHAR" "char" "CHAR"
   "json" "JSON" "jsonb" "JSON"
   "uuid" "CHAR(36)"
   "timestamp" "DATETIME" "datetime" "DATETIME"
   "int" "SIGNED" "integer" "SIGNED" "bigint" "SIGNED" "smallint" "SIGNED"
   "numeric" "DECIMAL" "decimal" "DECIMAL"
   "bool" "SIGNED" "boolean" "SIGNED"})

(def ^:private sqlite-cast-types
  "Translation table for a user's explicit `::cast` into SQLite's CAST()
  targets. SQLite has no `::`, and a CAST target only picks an affinity, so
  the names map to the five that matter. Dates and times map to TEXT, not
  DATE: a cast to DATE gets NUMERIC affinity, which would turn the text
  `2024-01-31` into the number 2024. Anything not listed is uppercased and
  passed through, as for the other dialects."
  {"text" "TEXT" "varchar" "TEXT" "char" "TEXT"
   "json" "TEXT" "jsonb" "TEXT" "uuid" "TEXT"
   "date" "TEXT" "timestamp" "TEXT" "datetime" "TEXT" "time" "TEXT"
   "int" "INTEGER" "integer" "INTEGER" "bigint" "INTEGER" "smallint" "INTEGER"
   "bool" "INTEGER" "boolean" "INTEGER"
   "numeric" "NUMERIC" "decimal" "NUMERIC"
   "real" "REAL" "float" "REAL" "double" "REAL"})

(defn- render-cast
  "Apply an explicit cast to an already-rendered expression, dialect-aware:
  Postgres's `expr::cast` suffix vs. MySQL's and SQLite's `CAST(expr AS
  TYPE)` wrapper."
  [expr cast]
  (case *dialect*
    :mysql (str "CAST(" expr " AS " (get mysql-cast-types (s/lower-case cast) (s/upper-case cast)) ")")
    :sqlite (str "CAST(" expr " AS " (get sqlite-cast-types (s/lower-case cast) (s/upper-case cast)) ")")
    (str expr "::" cast)))

(defn- column-ref-with-cast
  "A column reference, optionally cast - the WHERE-clause column side."
  [alias column cast]
  (let [ref (q alias column)]
    (if cast (render-cast ref cast) ref)))

(defn- auto-cast-placeholder
  "The `?` placeholder for a value whose column type demands an implicit
  cast pine adds on the user's behalf (as opposed to an explicit `::cast`
  the user wrote, which is rendered on the column side instead - see
  render-cast). Kept deliberately minimal for MySQL: :jsonb needs
  CAST(? AS JSON), but :uuid and :date render as a bare `?` - MySQL has no
  UUID type (the value's already a string) and Connector/J binds dates
  correctly without help, so every unneeded CAST is just a new
  syntax-error surface for no benefit."
  [value-type]
  (case *dialect*
    :mysql (case value-type :jsonb "CAST(? AS JSON)" "?")
    ;; SQLite stores what it is given: a json value is already text, a uuid
    ;; a string, and a date is written as text by the driver.
    :sqlite "?"
    (case value-type :jsonb "?::jsonb" :uuid "?::uuid" :date "?::timestamp" "?")))

(defn- render-operator
  "MySQL and SQLite have no ILIKE/NOT ILIKE - map to LIKE. SQLite's LIKE is
  already case-insensitive for ASCII letters (not for the rest of Unicode),
  and MySQL's follows the column's collation."
  [operator]
  (if (#{:mysql :sqlite} *dialect*)
    (case operator "ILIKE" "LIKE" "NOT ILIKE" "NOT LIKE" operator)
    operator))

(defn- json-path-params
  "The parameters that name a path inside a JSON value. Postgres takes one
  per key or index. MySQL takes one path expression, `$.\"address\".\"city\"`
  or `$.\"tags\"[0]`, with each key quoted so it can hold any character.
  Keys are typed by the user, so they're always parameters, never SQL text."
  [path]
  (if (= *dialect* :mysql)
    [(dt/string (apply str "$" (map #(if (integer? %) (str "[" % "]") (str "." (json/generate-string %))) path)))]
    (map #(dt/string (str %)) path)))

(defn- json-extract
  "The value at `path` inside the JSON column `col-ref`, as [sql params]:
  as JSON (`:json`), to compare or sort as JSON values, or as text
  (`:text`), to show or to match with like. A missing key is NULL. A plain
  Postgres `json` column is cast to `jsonb`, which a `jsonb` one already is."
  [as col-ref path]
  (let [params (json-path-params path)]
    [(if (= *dialect* :mysql)
       (let [x (str "JSON_EXTRACT(" col-ref ", ?)")]
         (if (= as :text) (str "JSON_UNQUOTE(" x ")") x))
       (str (if (= as :text) "jsonb_extract_path_text(" "jsonb_extract_path(")
            col-ref "::jsonb, " (s/join ", " (repeat (count params) "?::text")) ")"))
     params]))

(def ^:private mysql-json-types
  {"number" ["INTEGER" "DOUBLE" "DECIMAL" "UNSIGNED INTEGER"]
   "string" ["STRING"]
   "boolean" ["BOOLEAN"]})

(defn- json-type-check
  "SQL that is true when the JSON value `x` has the type `json-type`. Both
  databases order values of different JSON types instead of refusing to
  compare them: on Postgres `true > 10` and `{\"a\":1} > 10` are true. So
  `<` and `>` only compare values of the literal's own type."
  [x json-type]
  (if (= *dialect* :mysql)
    (str "JSON_TYPE(" x ") IN (" (s/join ", " (map #(str "'" % "'") (mysql-json-types json-type))) ")")
    (str "jsonb_typeof(" x ") = '" json-type "'")))

(defn- in-subquery
  "MySQL rejects `<verb> <target> ... WHERE id IN ( SELECT ... FROM <same
  target> ... )` on two counts: ER_UPDATE_TABLE_USED (1093) - can't select
  from the update/delete target in its own subquery - and error 1235,
  LIMIT isn't allowed inside an IN subquery (reachable whenever a user
  writes `| limit: N | delete!`). Wrapping the inner SELECT in a derived
  table sidesteps both. Identity for Postgres. The inner select holds
  exactly the columns being matched - one, or the several of a composite
  key - and nothing else, so SELECT * here passes them straight through."
  [sql]
  (case *dialect*
    :mysql (str "SELECT * FROM ( " sql " ) AS " (q "pine_sub"))
    sql))

(defn- join-column-ref
  "Render a join column, casting to text when the two sides of a heuristic
  join (a naming-convention guess, not a real FK) turn out to have
  different DB types - e.g. one side stored as varchar, the other as uuid.
  Real FK joins are never cast: the constraint already guarantees the types
  line up, so casting would just throw away index usage."
  [cast alias column]
  (let [ref (q alias column)]
    (if cast (render-cast ref cast) ref)))

(defn- build-on-clause
  "The ON condition of one join: every column pair the relation carries,
  ANDed together. A key made of several columns is simply a longer list
  here - this does not care how many there are.

  An unresolved join (nothing connects the two tables, so :columns is
  empty) never gets here: build-join-clause rejects it first."
  [{:keys [from to columns cast]}]
  (when (seq columns)
    (str " ON " (s/join " AND "
                        (map (fn [{from-column :from to-column :to}]
                               (str (join-column-ref cast from from-column)
                                    " = " (join-column-ref cast to to-column)))
                             columns)))))

(defn- table-label
  "schema.table (or just table) for the table behind alias `a`, for messages."
  [aliases a]
  (let [{:keys [schema table]} (get aliases a)]
    (if schema (str schema "." table) (or table a))))

(defn- check-joins-resolved
  "Throws when a join has nothing connecting its two tables. Such a join
  would render with no ON clause: a syntax error on Postgres, and on MySQL a
  cross join of every row with every row, which a delete! would then act on."
  [{:keys [joins aliases]}]
  (doseq [{:keys [from to columns]} joins
          :when (empty? columns)]
    (throw (ex-info (str "No relation between `" (table-label aliases from) "` and `" (table-label aliases to)
                         "`. Name the join column with `.column`, for example `"
                         (table-label aliases to) " .some_id`.")
                    {:error-type "unresolved-join" :from from :to to}))))

(defn- build-join-clause [{:keys [tables joins aliases] :as state}]
  (check-joins-resolved state)
  (when (not-empty (rest tables))
    (let [join-statements (map (fn [{:keys [to type] :as join}]
                                 (let [{to-table :table to-schema :schema} (get aliases to)
                                       join-keyword (if type (str type " JOIN") "JOIN")]
                                   (str join-keyword " " (q to-schema to-table) " AS " (q to)
                                        (build-on-clause join))))
                               ;; (reverse joins)
                               joins)]
      (s/join " " join-statements))))

(defn- column-sql
  "[sql params] for one column of a SELECT list, before its `AS` name."
  [state rules {:keys [column alias symbol col-fn path] :as col}]
  (cond
    (and (seq rules) (access-policy/sensitive-column? state rules col)) [access-policy/redacted-sql-literal []]
    path (json-extract :text (q alias column) path)
    ;; Column function (currently date functions)
    col-fn [(col-fn-expr col-fn (q alias column)) []]
    ;; Symbol-based columns (like aggregates)
    (empty? column) [(if alias (str (q alias) "." symbol) symbol) []]
    ;; Regular columns
    :else [(q alias column) []]))

(defn- build-columns-clause
  "{:sql :params} for the SELECT list."
  [{:keys [operation columns current] :as state}]
  (let [type (-> operation :type)
        rules (:access-policy state)
        restricted? (seq rules)
        ;; An explicit `select: alias.*` is just as opaque to per-column
        ;; redaction as the implicit current-table `.*` handled below -
        ;; expand it first so the rest of this function only ever sees real
        ;; columns (or, for a variable/CTE alias, is left untouched).
        ;; vec, not mapcat's lazy seq: the later `(into columns expanded)`
        ;; below relies on conj appending (vector semantics) - conj on a
        ;; plain seq prepends instead, which would silently put every
        ;; expanded/auto-id column in reverse order.
        columns (if restricted?
                  (vec (mapcat #(access-policy/expand-explicit-star state %) columns))
                  columns)
        ;; Separate auto-ID columns from user-selected columns
        {auto-id-columns true user-columns nil} (group-by #(:auto-id %) columns)
        ;; Check if any non-auto-ID columns are selected for the current table
        current-table-has-columns? (some #(= (:alias %) current) user-columns)
        star-eligible? (not (contains? #{:select :delete-action :group} type))
        ;; Under the access policy, a bare `current.*` is opaque to the
        ;; per-column check redaction depends on - expand it into an
        ;; explicit column list (pine.access-policy/expand-star) so each one
        ;; can be checked and redacted individually. Policy off: unchanged.
        expanded (when (and restricted? star-eligible? (not current-table-has-columns?))
                   (access-policy/expand-star state current))
        columns (if (seq expanded) (into columns expanded) columns)
        current-table-has-columns? (or current-table-has-columns? (seq expanded))
        select-all (cond
                     (not star-eligible?) ""
                     current-table-has-columns? ""  ; Don't add .* if current table has explicit columns
                     :else (str (if (seq columns) ", " "") (q current) ".*"))]
    (let [parts (map (fn [{:keys [column column-alias symbol] :as col}]
                       (let [redact? (and restricted? (access-policy/sensitive-column? state rules col))
                             [c params] (column-sql state rules col)
                             ;; A redacted literal loses Postgres' automatic naming of a
                             ;; bare column reference, so name it explicitly whenever no
                             ;; explicit column-alias was already going to do that job.
                             out-alias (or column-alias (when redact? (if (empty? column) symbol column)))]
                         [(if out-alias (str c " AS " (q out-alias)) c) params]))
                     columns)]
      {:sql (str "SELECT " (s/join ", " (map first parts)) select-all " FROM")
       :params (mapcat second parts)})))

(defn- build-order-clause
  "[sql params] for ORDER BY, or nil. A key inside a JSON column sorts as a
  JSON value, so numbers sort as numbers."
  [{:keys [order]}]
  (when (seq order)
    (let [parts (map (fn [{:keys [alias column direction path]}]
                       (let [[c params] (if path (json-extract :json (q alias column) path) [(q alias column) []])]
                         [(str c " " direction) params]))
                     order)]
      [(str "ORDER BY " (s/join ", " (map first parts))) (mapcat second parts)])))

(defn- remove-symbols
  "Remove symbols or columns from a vector of values"
  [vs]
  (filter #(not (#{:symbol :column :named-result} (:type %))) vs))

(defn- build-group-clause [{:keys [group]}]
  (if (empty? group) nil
      (str
       "GROUP BY "
       (s/join
        ", "
        ;; For each group column, determine the appropriate reference
        (map (fn [{:keys [alias column column-alias col-fn path]}]
               (if (or col-fn path)
                 ;; Use the column alias for columns with functions applied,
                 ;; and for keys inside a JSON column: the SELECT list names them
                 (q column-alias)
                 ;; Use the full qualified column for regular columns
                 (q alias column)))
             group)))))

(defn- render-comparison
  "`<column-sql> <operator> <value>`, with the column already rendered."
  [column-sql cast operator value]
  (cond
    ;; `in <named result>`: the values its one column returns (pine.ast.where
    ;; filled in :column). Its CTE is emitted by all-ctes.
    (= (:type value) :named-result)
    (str column-sql " " (render-operator operator) " ( SELECT " (q (:column value)) " FROM " (q (:value value)) " )")

    (or (= operator "IN") (= operator "NOT IN"))
    ;; A map here is an unbound `in $variable` (pine.variables): one `?`
    ;; standing for the list, shown as `$name` by formatted-query.
    (str column-sql " " (render-operator operator) " ("
         (if (map? value) "?" (s/join ", " (repeat (count value) "?"))) ")")
    :else
    (str column-sql " " (render-operator operator) " "
         (cond
           (= (:type value) :symbol) (:value value)
           (= (:type value) :column) (let [[a col] (:value value)] (q a col))
           ;; Cast the parameter/value, not the column (unless explicit cast)
           :else (if cast "?" (auto-cast-placeholder (:type value)))))))

(defn- value-params
  "The parameters a condition's value binds: none for a symbol, a column or
  a named result, one per item of an `in` list."
  [value]
  (->> [value] remove-symbols flatten))

(defn- render-path-condition
  "[sql params] for a condition on a key inside a JSON column. `=`, `!=`,
  `<` and `>` compare JSON values (pine.ast.where made the literal one).
  `like`, `in` and `is null` compare the value as text."
  [{alias :alias col :column :keys [cast operator value path]}]
  (let [ref (q alias col)
        [json-x json-params] (json-extract :json ref path)
        [text-x text-params] (json-extract :text ref path)]
    (cond
      (and (#{">" "<" ">=" "<="} operator) (:json-type value))
      [(str "(" (json-type-check json-x (:json-type value)) " AND " json-x " " operator " " (auto-cast-placeholder :jsonb) ")")
       (concat json-params json-params (value-params value))]

      (= :jsonb (:type value))
      [(str json-x " " operator " " (auto-cast-placeholder :jsonb)) (concat json-params (value-params value))]

      ;; MySQL's JSON_UNQUOTE turns a JSON null into the text 'null', so
      ;; missing and null are told apart by JSON_TYPE instead.
      (and (= *dialect* :mysql) (#{"IS" "IS NOT"} operator))
      [(str "COALESCE(JSON_TYPE(" json-x "), 'NULL') " (if (= operator "IS") "=" "<>") " 'NULL'") json-params]

      :else
      [(render-comparison (if cast (render-cast text-x cast) text-x) cast operator value)
       (concat text-params (value-params value))])))

(defn- render-condition
  "[sql params] for one condition."
  [{alias :alias col :column :keys [cast operator value path] :as condition}]
  (if path
    (render-path-condition condition)
    [(render-comparison (column-ref-with-cast alias col cast) cast operator value) (value-params value)]))

(defn- build-where-clause
  "[sql params] for WHERE, or nil."
  [where]
  (when (not-empty where)
    (let [parts (for [entry where]
                  ;; A {:or [...]} entry is the conditions of one where:
                  ;; segment joined with `or` -- one parenthesized OR group.
                  (if-let [conditions (:or entry)]
                    (let [rendered (map render-condition conditions)]
                      [(str "(" (s/join " OR " (map first rendered)) ")") (mapcat second rendered)])
                    (render-condition entry)))]
      [(str "WHERE " (s/join " AND " (map first parts))) (mapcat second parts)])))

(defn- build-bare-select [state]
  ;; Every SELECT Pine builds goes through here, named results' bodies
  ;; included, so this is where hidden columns are kept out of where:,
  ;; order: and group:.
  (access-policy/check-references state (:access-policy state))
  (let [{:keys [tables _columns limit where aliases]} state
        from         (let [{a :alias} (first tables)
                           {table :table schema :schema} (get aliases a)]
                       (str (q schema table) " AS " (q a)))
        join         (build-join-clause state)
        select       (build-columns-clause state)
        [where-clause where-params] (build-where-clause where)
        group (build-group-clause state)
        [order order-params] (build-order-clause state)
        limit (when limit (str "LIMIT " limit))
        query (s/join " " (filter some? [(:sql select) from join where-clause group order limit]))
        ;; In the order their `?` appear in the query.
        params (concat (:params select) where-params order-params)]

    {:query query :params (seq params)}))

(defn- build-cte-body
  "Generate the inner SQL for a variable's AST used as a CTE.
  When the current table has no explicit user columns, .* is added and already
  includes the key - so the hidden key columns are dropped to avoid duplicate
  names. When explicit columns are present (no .*), they are kept but their
  alias is stripped so the key is accessible for join conditions.
  Returns {:query ... :params ...}."
  [ast]
  (let [current-alias    (:current ast)
        user-columns     (remove :auto-id (:columns ast))
        has-explicit?    (some #(= (:alias %) current-alias) user-columns)
        columns          (keep (fn [col]
                                 (if (and (:auto-id col) (= (:alias col) current-alias))
                                   (when has-explicit? (dissoc col :column-alias))
                                   col))
                               (:columns ast))]
    (build-bare-select (assoc ast :columns columns))))

(declare all-ctes)

(defn- dedupe-ctes [ctes]
  (->> ctes
       (reduce (fn [[seen acc] [name _ _ :as cte]]
                 (if (contains? seen name)
                   [seen acc]
                   [(conj seen name) (conj acc cte)]))
               [#{} []])
       second))

(defn- cte-for
  "A named result's own CTE, after every CTE it needs itself."
  [name ast]
  (let [{:keys [query params]} (build-cte-body ast)]
    (conj (vec (all-ctes ast)) [name query params])))

(defn- collect-ctes
  "Recursively collect [name query params] triples from variable tables in
  topological order (deepest dependencies first). Deduplicates by name."
  [tables aliases]
  (->> tables
       (mapcat (fn [{:keys [alias]}]
                 (let [entry (get aliases alias)]
                   (when-let [ast (:ast entry)]
                     (cte-for (:table entry) ast)))))
       dedupe-ctes))

(defn- all-ctes
  "Every CTE a state needs: the named results it uses as tables, and those
  it uses after `in` (pine.ast.where's :value-ctes), each after its own
  dependencies. Deduplicates by name."
  [state]
  (dedupe-ctes (concat (collect-ctes (:tables state) (:aliases state))
                       (mapcat (fn [[name ast]] (cte-for name ast)) (:value-ctes state)))))

(defn build-select-query [state]
  (let [ctes        (all-ctes state)
        result      (build-bare-select state)
        cte-params  (mapcat #(nth % 2 nil) ctes)
        cte-prefix  (when (seq ctes)
                      (str "WITH "
                           (s/join ", " (map (fn [[name body _]]
                                               (str (q name) " AS ( " body " )"))
                                             ctes))
                           " "))]
    (-> result
        (update :query  #(str cte-prefix %))
        (update :params #(seq (concat cte-params %))))))

(defn build-count-query [state]
  (let [{:keys [query params]} (build-select-query state)]
    {:query (str "WITH x AS ( " query " ) SELECT COUNT(*) FROM x")
     :params params}))

(defn- build-inner-select-for-group
  "Build the inner SELECT for a GROUP query CTE. Includes non-aggregate columns only."
  [state]
  (access-policy/check-references state (:access-policy state))
  (let [{:keys [tables columns where aliases joins]} state
        {a :alias} (first tables)
        {table :table schema :schema} (get aliases a)
        ;; Filter out aggregate function columns (those with :symbol but no :column)
        non-aggregate-cols (filter #(or (:column %) (:auto-id %)) columns)
        ;; Create a temporary state for building the SELECT clause with only non-aggregate columns
        temp-state (assoc state
                          :columns non-aggregate-cols
                          :operation {:type :group})
        ;; Build SELECT clause using the same logic as regular queries, but add aliases to all columns
        rules (:access-policy state)
        select-parts (map (fn [{:keys [column column-alias] :as col}]
                            (let [[c params] (column-sql state rules col)
                                  ;; Always use an alias: either column-alias or column name
                                  col-alias (or column-alias column)]
                              [(str c " AS " (q col-alias)) params]))
                          non-aggregate-cols)
        select-clause (str "SELECT " (s/join ", " (map first select-parts)))
        from (str "FROM " (q schema table) " AS " (q a))
        join (build-join-clause {:tables tables :joins joins :aliases aliases})
        [where-clause where-params] (build-where-clause where)]
    {:query (s/join " " (filter some? [select-clause from join where-clause]))
     :params (concat (mapcat second select-parts) where-params)}))

(defn- build-outer-select-for-group
  "Build the outer SELECT for a GROUP query. References CTE columns and includes aggregates."
  [cte-alias {:keys [columns group]}]
  (let [;; Get group columns - use column-alias if present, otherwise column name
        group-cols (map #(or (:column-alias %) (:column %)) group)
        select-items (map (fn [{:keys [column column-alias symbol col-fn]}]
                            (cond
                             ;; Aggregate function (has symbol, no column)
                              (and symbol (empty? column))
                              (if column-alias
                                (str symbol " AS " (q column-alias))
                                symbol)
                             ;; Non-aggregate column - reference from CTE
                             ;; Use the same alias that was assigned in the inner query
                              :else (q cte-alias (or column-alias column))))
                          columns)
        group-by (str "GROUP BY " (s/join ", " (map #(q cte-alias %) group-cols)))]
    {:select (str "SELECT " (s/join ", " select-items) " FROM " (q cte-alias))
     :group-by group-by}))

(defn build-group-query [state]
  (let [{:keys [index tables aliases]} state
        ;; A checkpoint/variable feeding into a terminal GROUP (e.g. `|= x |
        ;; g: ...`) needs its own CTE emitted too -- this path used to skip
        ;; collect-ctes entirely (unlike build-select-query, which already
        ;; calls it), so the user-named CTE was never defined and the group's
        ;; wrapper CTE (below) referenced it as a dangling bare relation.
        ctes        (all-ctes state)
        cte-params  (mapcat #(nth % 2 nil) ctes)
        cte-prefix  (when (seq ctes)
                      (str (s/join ", " (map (fn [[name body _]]
                                               (str (q name) " AS ( " body " )"))
                                             ctes))
                           ", "))
        cte-alias (str "x_" index)
        ;; Build inner query (base SELECT with non-aggregate columns)
        {inner-query :query inner-params :params} (build-inner-select-for-group state)
        ;; Build outer query (SELECT from CTE with aggregates and GROUP BY)
        {:keys [select group-by]} (build-outer-select-for-group cte-alias state)
        ;; Combine into CTE
        query (str "WITH " cte-prefix (q cte-alias) " AS ( " inner-query " ) " select " " group-by)
        params inner-params]
    {:query query :params (seq (concat cte-params params))}))

(defn- scoped?
  "Whether a state's rows are narrowed by anything: a where:, a limit:, or a
  source table that is a named result or checkpoint CTE whose own query is
  narrowed. `company | l: 10 | employee` keeps no limit in its final state -
  the limit was sealed into a CTE - but its rows are still scoped by it."
  [{:keys [where limit tables aliases]}]
  (boolean
   (or (seq where)
       limit
       (some (fn [{a :alias}]
               (when-let [cte (get-in aliases [a :ast])]
                 (scoped? cte)))
             tables))))

(defn- refuse-write [message]
  (throw (ex-info message {:error-type "write-refused"})))

(defn- check-write-allowed
  "Refuses a delete!/update! that would act on more than the person can have
  meant. `target` is the alias of the table being changed."
  [{:keys [group aliases] :as state} op-name target]
  (when (get-in aliases [target :ast])
    (refuse-write (str op-name " can't change a named result. Pipe it onto the table itself.")))
  (when (seq group)
    (refuse-write (str op-name " after group: is not supported. Remove the group:, or select the rows with where:.")))
  (when-not (scoped? state)
    (refuse-write (str "Refusing to change every row of `" (table-label aliases target)
                       "`. Add a where: or a limit: first."))))

(defn- key-target
  "The left side of `... IN ( SELECT ... )` for the columns that identify a
  row: one column, or several matched as a row, `(a, b)`."
  [columns]
  (if (next columns)
    (str "(" (s/join ", " (map q columns)) ")")
    (q (first columns))))

(defn build-delete-query
  "`delete!` names the columns that identify the rows to remove, and the
  DELETE matches them against those same columns as selected by the
  expression it is piped onto.

  More than one column is matched as a row: `WHERE (a, b) IN ( SELECT a, b
  ... )`. A table whose key is composite has no single column that picks
  out a row on its own, so deleting on one of them at a time would take
  rows belonging to other records with it."
  [state]
  (check-write-allowed state "delete!" (:current state))
  (let [{:keys [delete current aliases]} state
        {table :table schema :schema}     (get aliases current)
        {:keys [columns]}                 delete
        state                             (assoc state :columns
                                                 (mapv (fn [column] {:column column :alias current}) columns))
        {:keys [query params]}            (build-select-query state)]
    {:query (str "DELETE FROM " (q schema table) " WHERE " (key-target columns) " IN ( "  (in-subquery query) " )")
     :params params}))

(defn- update-source-column
  "SQL for a column on the right of an `update!` assignment. UPDATE has no
  alias in scope, so the column is written bare, and it has to belong to the
  table being updated."
  [update-alias {[alias column] :value}]
  (when (and alias (not= alias update-alias))
    (throw (ex-info (str "update! can only copy a column of the table it changes. `"
                         alias "." column "` is from another table.")
                    {:alias alias :column column})))
  (q column))

(defn- update-key
  "The columns update! finds a table's rows by: its primary key. Refuses a
  table that has none, such as a view: no column is known to pick out one
  row, so the update could change rows nobody meant."
  [{:keys [aliases row-keys]} update-alias]
  (or (get row-keys update-alias)
      (refuse-write (str "update! can't change `" (table-label aliases update-alias)
                         "`: it has no primary key, so there is no way to tell its rows apart."))))

(defn- build-single-update-query [state update-alias assignments]
  (check-write-allowed state "update!" update-alias)
  (let [{:keys [aliases]}              state
        {table :table schema :schema}  (get aliases update-alias)
        key-columns                    (update-key state update-alias)
        set-clause (s/join ", "
                           (map (fn [{:keys [column value]}]
                                  (let [{:keys [alias column]} column]
                                    (str (q column) " = " (cond
                                                            (= (:type value) :symbol) (:value value)
                                                            (= (:type value) :column) (update-source-column update-alias value)
                                                            :else (auto-cast-placeholder (:type value))))))
                                assignments))
        state-for-subquery (-> state
                               (assoc :columns (mapv (fn [column] {:column column :alias update-alias}) key-columns))
                               (assoc :operation {:type :select :value nil}))
        {:keys [query params]} (build-select-query state-for-subquery)
        update-params (->> assignments
                           (map :value)
                           (filter #(not (or (= (:type %) :symbol) (= (:type %) :column)))))]
    {:table (if schema (str schema "." table) table)
     :query (str "UPDATE " (q schema table) " SET " set-clause " WHERE " (key-target key-columns)
                 " IN ( " (in-subquery query) " )")
     :params (concat update-params params)}))

(defn build-update-queries [state]
  "Returns a list of {:table table-name :query query :params params}, one per table being updated."
  (let [{:keys [update current aliases]} state
        {:keys [assignments]}             update
        ;; Group assignments by table alias (use current when column has no alias)
        grouped (group-by (fn [{:keys [column]}]
                            (or (:alias column) current))
                          assignments)]
    (mapv (fn [[update-alias table-assignments]]
            (build-single-update-query state update-alias table-assignments))
          grouped)))

;; The same ceiling as limit: (pine.parser/max-limit).
(def ^:private max-group-rows 10000)

(defn build-query [state]
  (binding [*dialect* (connections/get-dialect (:connection-id state))]
    (let [{:keys [type]} (state :operation)]
      (cond
        (let [cur (-> state :current)]
          (or (nil? cur)
              (= "" (get-in state [:aliases cur :table])))) {:query "" :params nil}
        (= type :delete-action) (build-delete-query state)
        (= type :update-action) {:queries (build-update-queries state)}
        ;; A trailing comma: the assignment is still being typed. Nothing to
        ;; build, and run-query refuses to run it.
        (= type :update-partial) {:query "" :params nil}
        (= type :count) (build-count-query state)
        ;; A terminal group: returns at most as many groups as limit: allows.
        ;; A group sealed into a checkpoint CTE is built elsewhere and keeps
        ;; every group, since what follows it may narrow them.
        (= type :group) (update (build-group-query state) :query str " LIMIT " max-group-rows)
        ;; :paths only generates candidate pine expressions (see hints.paths) -
        ;; it never builds a query of its own.
        (= type :paths) {:query " /* No SQL. Pick a path from hints.paths and build that expression instead */ "}
        :else (build-select-query (update state :limit #(or % 250)))))))

(defn- param-preview [{v :value :as param}]
  (case (:type param)
    :boolean (str v)
    ;; Not run, only shown: a $variable with no value yet.
    :variable (str "$" v)
    (str "'" (s/replace (str v) "'" "''") "'")))

(defn- fill-params
  "The query with each `?` replaced by its param, in order, in one pass. Never
  rescans what it inserted, so a `?` inside a value stays in the value."
  [query params]
  (let [parts (s/split query #"\?" -1)]
    (apply str (first parts)
           (map (fn [part param] (str (if param (param-preview param) "?") part))
                (rest parts)
                (concat params (repeat nil))))))

(defn formatted-query [build-result]
  (if-let [queries (:queries build-result)]
    ;; Multiple update queries
    (s/join "\n" (map (fn [{:keys [query params]}]
                        (if (empty? query) "" (str (fill-params query params) ";")))
                      queries))
    ;; Single query (legacy format or other operations)
    (let [{:keys [query params]} build-result]
      (if (empty? query) "" (str "\n" (fill-params query params) ";\n")))))

(defn run-query [state]
  (if (= (-> state :operation :type) :no-op)
    [["No operation"] ["-"]]
    (let [connection-id (state :connection-id)
          operation-type (-> state :operation :type)
          _ (when (= operation-type :update-partial)
              (throw (ex-info "The update! isn't finished: add an assignment after the comma, or remove the comma."
                              {:error-type "incomplete"})))
          build-result  (build-query state)]
      (cond
        ;; Every write runs in a transaction, even a single statement: if it
        ;; fails part way, nothing is left half-applied. A multi-table
        ;; update! rolls back all of its tables together.
        (= :update-action operation-type)
        (let [results (db/run-action-queries-in-transaction connection-id (:queries build-result))]
          (into [["Table" "Rows updated"]]
                (map (fn [[t n]] [t n]) results)))

        (= :delete-action operation-type)
        (let [{:keys [query params]} build-result
              [[_ affected-rows]] (db/run-action-queries-in-transaction
                                   connection-id [{:table nil :query query :params params}])]
          [["Rows deleted"] [affected-rows]])

        :else
        ;; Select and other operations
        (db/run-query connection-id (select-keys build-result [:query :params]))))))
