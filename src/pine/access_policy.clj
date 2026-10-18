(ns pine.access-policy
  "Per-connection, data-driven access policy for restricted evaluation.

  A policy is a vector of rule maps, each carrying a :type discriminant
  (a string, since rules travel as JSON over HTTP -- see api.clj's
  :access-policy param). Most rules show a column. A `hide` rule wraps one
  of those matchers and hides what it matches, whatever any show rule says:

    {:type \"hide\" :match {:type \"column-name\" :contains \"token\"}}

  It is a type of its own, not a flag on the others, because an older
  pine-lang ignores fields it doesn't know and would read a flagged rule as
  a show rule. An unknown type never matches, so the column stays hidden.

  When a request's :access-policy is non-empty, every SELECT column is
  checked against it (see decide). A hidden column is rendered as a fixed
  literal directly in the generated SQL, so the real value never leaves the
  database.

  This module carries zero policy *content* of its own -- no hardcoded
  allow-list, no notion of MCP. It only knows how to evaluate whatever
  rules a caller supplies. Deciding what a connection's rules default to,
  and letting an owner relax them, is entirely the caller's job (today:
  beamlynx-desktop's credential-store.ts)."
  (:require
   [clojure.string :as s]
   [pine.computed :as computed]
   [pine.data-types :as dt]))

(def redacted-value "xxxxx")
(def redacted-sql-literal (str "'" redacted-value "'"))

(defn sanitize-rules
  "Coerce a raw, untrusted :access-policy value (parsed straight from an
  HTTP JSON body) into a vector of well-shaped rule maps, dropping anything
  that isn't a map with a string :type instead of erroring the request over
  a malformed rule. Not a security boundary -- the caller who sends this
  param could just as easily send an empty one to turn the policy off
  outright, so there's nothing to defend against here beyond keeping a bad
  request from crashing eval/build."
  [raw]
  (if (sequential? raw)
    (filterv #(and (map? %) (string? (:type %))) raw)
    []))

(defn annotate-mode?
  "Whether a request asked only to be told how the policy treats each result
  column, with nothing masked or refused: `access-policy-mode` is exactly
  \"annotate\". Anything else, absent or misspelled, enforces. It lets the
  owner's own unmasked tab show which columns an agent can't see. Not a
  security boundary for the same reason sanitize-rules isn't one: a caller
  could as well send no rules."
  [params]
  (= "annotate" (:access-policy-mode params)))

(defn- column-db-type
  "Resolve the real Postgres type for a SELECT column descriptor, using its
  own already-resolved :source (set by ast/select.clj/ast/group.clj) when
  present, falling back to the table alias's own entry in state's :aliases.
  Returns nil when it can't be resolved at all (a computed/aggregate
  expression with no real :column, or a table pine has no schema info for)."
  [{:keys [references aliases]} alias column source]
  (when-not (s/blank? column)
    (when-let [table-info (or source (get aliases alias))]
      (dt/get-column-type references alias column table-info))))

(defn- column-table-info
  "The {:table :schema} a SELECT column descriptor's own real table -- same
  :source/:aliases fallback column-db-type uses, but returning the table
  itself rather than looking up one column's type on it."
  [{:keys [aliases]} alias source]
  (or source (get aliases alias)))

(defn- resolved-schema
  "The schema of a column's real table. An unqualified table resolves the
  way pine.db.references' resolve-bare-tables resolves it: the one schema
  that has the table, or `public` among several. nil when that is still
  ambiguous."
  [references {:keys [table schema]}]
  (or schema
      (let [schemas (keep (fn [[s data]] (when (get-in data [:table table]) s)) (:schema references))]
        (cond
          (= 1 (count schemas))           (first schemas)
          (some #{"public"} schemas)      "public"
          :else                           nil))))

(def ^:private catalog-schemas
  "Postgres's own schema-introspection views, not application data. The MCP
  tool surface (beamlynx-desktop's find_tables/complete_query, and their own
  descriptions) explicitly directs an agent to query information_schema.*
  directly for schema discovery -- redacting it would silently defeat that
  and return `xxxxx` for real table/column names, which are not sensitive
  values in the first place. Exempt structurally, the same as an auto-id
  column, not via a configurable rule: there's no legitimate policy under
  which catalog metadata should be masked.

  Only the relations schema discovery needs, not the whole schemas: they
  also hold pg_stat_activity (the text of every running query, literal
  values included), pg_shadow and pg_authid (password hashes), and
  pg_settings. Those are treated like any other table."
  {"information_schema" #{"tables" "columns" "key_column_usage" "table_constraints"
                          "referential_constraints" "constraint_column_usage" "schemata" "views"}
   "pg_catalog"         #{"pg_class" "pg_attribute" "pg_namespace" "pg_type" "pg_constraint"
                          "pg_index" "pg_description" "pg_tables" "pg_views" "pg_indexes"}})

(defn catalog-relation?
  "Whether schema.table is one of the catalog relations exempt from the policy."
  [schema table]
  (contains? (get catalog-schemas schema) table))

(defn- catalog-column?
  [state alias source]
  (let [{:keys [schema table]} (column-table-info state alias source)]
    (catalog-relation? schema table)))

(defn- foreign-key-source-column?
  "Whether `column` is a source column of a detected foreign-key or
  heuristic relation for `table` -- i.e. pine-lang's own join-relation
  index (pine.db.references' index-foreign-keys/index-heuristic-relations,
  both of which populate the same [:table T :refers-to ...] shape) already
  treats it as identifier-shaped, real FK or naming-convention guess alike.
  A lookup against index pine-lang builds anyway, not new indexing."
  [references table column]
  (boolean
   (some (fn [[_target-table entry]] (contains? (:via entry) column))
         (get-in references [:table table :refers-to]))))

(defn- name-matches?
  "Whether a column name matches a column-name matcher's `suffix`,
  `contains` or `equals`, ignoring case: `accessToken`, `access_token` and
  `ACCESS_TOKEN` all contain `token`. A matcher with none of the three
  matches nothing."
  [column {:keys [suffix contains equals]}]
  (let [column (s/lower-case column)
        lower  #(when (and (string? %) (not (s/blank? %))) (s/lower-case %))]
    (boolean
     (or (some->> (lower suffix) (s/ends-with? column))
         (some->> (lower contains) (s/includes? column))
         (some->> (lower equals) (= column))))))

(defn- matcher-matches?
  "Whether a matcher (a show rule, or the :match of a hide rule, with
  `hiding?` true) covers this column. Unrecognized :type values never
  match."
  [state {:keys [column alias source]} matcher hiding?]
  (case (:type matcher)
    "column-type"
    (contains? (set (:allow matcher)) (column-db-type state alias column source))

    "foreign-key"
    (let [{:keys [table]} (column-table-info state alias source)]
      (and table (not (s/blank? column))
           (foreign-key-source-column? (:references state) table column)))

    "column-name"
    (and (not (s/blank? column)) (name-matches? column matcher))

    ;; One exact column of one real table: what the results grid's
    ;; right-click menu writes. A rule without :schema matches the table in
    ;; any schema. When the table's schema can't be told, a hide rule
    ;; matches and a show rule doesn't, so the doubt keeps it hidden.
    "column"
    (let [{:keys [table] :as table-info} (column-table-info state alias source)
          schema (when table (resolved-schema (:references state) table-info))]
      (boolean
       (and table (= table (:table matcher)) (= column (:column matcher))
            (or (nil? (:schema matcher))
                (= schema (:schema matcher))
                (and hiding? (nil? schema))))))

    false))

(defn- hide-rule? [rule] (= "hide" (:type rule)))

(def ^:private known-matchers #{"column-type" "foreign-key" "column-name" "column"})

(defn- hide-matches?
  "Whether a hide rule covers this column. A hide rule whose matcher this
  version doesn't know (a newer client's matcher, or a malformed one) covers
  every column. Treating it as matching nothing would let a show rule expose
  what the hide rule was written to protect. For a hide rule, failing closed
  means hiding."
  [state col {:keys [match]}]
  (if (and (map? match) (contains? known-matchers (:type match)))
    (matcher-matches? state col match true)
    true))

(defn decide
  "How the policy treats one SELECT column descriptor: {:hidden bool}, plus
  :by, the rule that decided it, when one did. Only meaningful when `rules`
  is non-empty.

  Hide rules are checked first. A match hides the column whatever any show
  rule says: a hide rule is a guarantee. Then any show rule shows it.
  Otherwise it is hidden. Unknown defaults to protected, so a column pine
  can't resolve a type or table for stays hidden. A raw aggregate like
  string_agg(email, ',') would otherwise be a one-line bypass.

  Postgres's own catalog views (see catalog-schemas) are always shown, hide
  rules included: they hold names, not values. The hidden `id` column pine
  adds for editing is shown too, unless a hide rule covers it. Other hidden key columns are not exempt: a primary key can be any
  column, `email` included. ast/select.clj leaves them out instead when the
  policy hides them.

  A computed value (or an aggregate) is shown only when every column it
  reads is. Otherwise a condition on a hidden column would leak through its
  true/false result. One that reads no column, like now(), is shown."
  [state rules {:keys [alias source auto-id column] :as col}]
  (if-let [node (:computed col)]
    {:hidden (boolean (some #(:hidden (decide state rules (select-keys % [:alias :column :source])))
                            (computed/columns node)))}
    (let [hide (when-not (catalog-column? state alias source)
                 (some #(when (and (hide-rule? %) (hide-matches? state col %)) %) rules))]
      (cond
        hide                                 {:hidden true :by hide}
        (catalog-column? state alias source) {:hidden false}
        (and auto-id (= "id" column))        {:hidden false}
        :else
        (if-let [show (some #(when (and (not (hide-rule? %)) (matcher-matches? state col % false)) %) rules)]
          {:hidden false :by show}
          {:hidden true})))))

(defn sensitive-column?
  "Whether a SELECT column descriptor should be redacted under `rules`. See
  decide. Callers gate on `rules` being non-empty themselves: an empty or
  absent policy means no redaction at all."
  [state rules col]
  (:hidden (decide state rules col)))

(defn annotate-column
  "A result column with :policy added, saying how `rules` treat it, so a
  client can show it and edit the policy from it: {:hidden :by :table}.
  :table is the column's real {:schema :table}, which is what a `column`
  rule names. A computed value gets :hidden only.
  Left without :policy when it isn't a column of a real table: a column of a
  named result or checkpoint (its body was checked at its source), or the
  bare alias get-columns appends."
  [{:keys [aliases] :as state} rules col]
  (if-not (map? col)
    col
    ;; A column of an implicit `SELECT *` is named from the result's header
    ;; row, as a keyword (api.clj's get-columns).
    (let [{:keys [alias source] :as col} (update col :column #(if (keyword? %) (name %) %))
          table-info (column-table-info state alias source)]
      (cond
        (:computed col)
        (assoc col :policy (decide state rules col))

        (and (:table table-info) (not (:ast (get aliases alias))) (not (s/blank? (:column col))))
        (assoc col :policy (assoc (decide state rules col)
                                  :table {:schema (resolved-schema (:references state) table-info)
                                          :table  (:table table-info)}))

        :else col))))

(defn- dedupe-by-column-name
  "Keep only the first entry for each :column name, preserving order.
  Needed because references' schema-less [:table table :columns] bucket
  aggregates columns across every schema that has a same-named table (see
  index-columns in pine.db.references) - an unqualified alias whose table name
  happens to exist in more than one schema would otherwise expand into the
  same column name twice."
  [columns]
  (->> columns
       (reduce (fn [{:keys [seen out]} col]
                 (if (contains? seen (:column col))
                   {:seen seen :out out}
                   {:seen (conj seen (:column col)) :out (conj out col)}))
               {:seen #{} :out []})
       :out))

(defn- real-columns
  "All real columns for a table alias, in schema order, from the
  connection's indexed references."
  [{:keys [references aliases]} alias]
  (let [{:keys [table schema]} (get aliases alias)]
    (dedupe-by-column-name
     (if schema
       (get-in references [:schema schema :table table :columns])
       (get-in references [:table table :columns])))))

(defn expand-star
  "Explicit stand-in for `alias.*`, used only under the access policy: every
  real column for the alias's table, in schema order, shaped like an
  explicitly-selected column (ast/select.clj) so sensitive-column? and SQL
  rendering treat it identically to one the user actually typed. A bare
  `alias.*` is otherwise opaque to the per-column check redaction depends
  on.

  Returns nil when alias resolves to a variable/CTE rather than a real
  table, so a bare `alias.*` is left as-is in that case: the CTE's own
  inner query, built under the same policy, already redacted anything
  sensitive at its source (see build-cte-body in eval.clj), so selecting
  `.*` from it re-selects already-safe values - nothing left to redact a
  second time, only real columns pine has no schema info for (a variable's
  :columns) that this function has no way to enumerate."
  [{:keys [aliases] :as state} current]
  (let [{:keys [table schema] :as table-info} (get aliases current)]
    (when (and table-info (not (:ast table-info)) (not (catalog-relation? schema table)))
      (let [columns (real-columns state current)]
        ;; Fail closed. With no indexed columns there is nothing to check
        ;; one by one, and the caller used to fall back to an unredacted
        ;; `alias.*`. That happened for a table created after the
        ;; connection was indexed, a MySQL table in another database, and
        ;; any catalog relation not listed in catalog-schemas.
        (when (empty? columns)
          (throw (ex-info (str "`" (if schema (str schema "." table) table)
                               "` isn't in the schema index, so the access policy can't check its columns. "
                               "Re-index the connection, or query a table it knows.")
                          {:error-type "policy" :table table :schema schema})))
        (mapv (fn [{:keys [column]}]
                {:column column :alias current :source {:table table :schema schema}})
              columns)))))

(defn expand-explicit-star
  "`select: alias.*` (as opposed to the implicit current-table default
  handled by expand-star) parses to a column descriptor with an empty
  :column and :symbol \"*\" (see pine.parser's :star rule). It's just as
  opaque to per-column redaction as the implicit form, so expand it the
  same way. Returns a seq of one or more column descriptors: the expansion
  when it succeeds, or the original descriptor unchanged (not real-column
  data, so nothing to expand and nothing to redact by name) when it can't
  be expanded - alias resolves to a variable/CTE rather than a real table."
  [state {:keys [column symbol alias] :as col}]
  (if (and (empty? column) (= symbol "*"))
    (or (seq (expand-star state alias)) [col])
    [col]))

(defn- entry-columns
  "[alias column] for an order:, group: or per entry: the column, or every
  column a computed column's value reads."
  [{:keys [alias column computed]}]
  (if computed
    (map (juxt :alias :column) (computed/columns computed))
    [[alias column]]))

(defn- where-columns
  "[alias column] for every column a where: entry names: the compared column
  (or the columns of a computed column's value), and a column or a computed
  value on the right of the comparison (whose alias defaults to the current
  table)."
  [current entry]
  (mapcat (fn [{:keys [value] :as condition}]
            (cond-> (vec (entry-columns condition))
              (= :column (:type value))
              (conj (let [[a c] (:value value)] [(or a current) c]))
              (= :computed (:type value))
              (into (map (juxt :alias :column) (computed/columns (:node value))))))
          (if-let [conditions (:or entry)] conditions [entry])))

(defn check-references
  "Throws when where:, order:, group: or `limit: N per` names a column the
  policy hides.
  Redaction only touches the SELECT list, so filtering, sorting or grouping
  on a hidden column still read its values: `where: title like 'a%'`
  repeated letter by letter recovers a hidden title from row counts alone.

  Exempt: the `id` column, unless a hide rule covers it (rows are already
  shown with their id), columns of a named result or checkpoint (its query was built under the same policy,
  so a hidden column there already holds the redacted literal), and
  catalog relations."
  [{:keys [where order group limit-per current aliases] :as state} rules]
  (when (seq rules)
    (doseq [[alias column] (concat (mapcat #(where-columns current %) where)
                                   (mapcat entry-columns order)
                                   (mapcat entry-columns group)
                                   (mapcat entry-columns (:columns limit-per)))
            :let [col {:alias alias :column column :auto-id (= "id" column)}]
            :when (and (not (s/blank? column))
                       (not (get-in aliases [alias :ast]))
                       ;; :auto-id lets `id` through the way the hidden key
                       ;; column is, which a hide rule still overrides.
                       (sensitive-column? state rules col))]
      ;; :hidden says which real column was refused, and the rule that hid
      ;; it, so a client can ask the owner to allow it (see api.clj's
      ;; error-body). Same shape as annotate-column's :policy.
      (throw (ex-info (str "`" alias "." column "` is hidden by the access policy, so it can't be used in "
                           "where:, order:, group: or after `per`.")
                      {:error-type "policy" :alias alias :column column
                       :hidden (:policy (annotate-column state rules col))})))))
