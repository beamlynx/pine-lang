(ns pine.access-policy
  "Per-connection, data-driven access policy for restricted evaluation.

  A policy is a vector of rule maps, each carrying a :type discriminant
  (a string, since rules travel as JSON over HTTP -- see api.clj's
  :access-policy param). When a request's :access-policy is non-empty,
  every SELECT column is checked against it: a column is shown as-is if
  it's the structural auto-id column, or any rule matches; otherwise it's
  rendered as a fixed literal directly in the generated SQL, so the real
  value never leaves the database.

  This module carries zero policy *content* of its own -- no hardcoded
  allow-list, no notion of MCP. It only knows how to evaluate whatever
  rules a caller supplies. Deciding what a connection's rules default to,
  and letting an owner relax them, is entirely the caller's job (today:
  beamlynx-desktop's credential-store.ts)."
  (:require
   [clojure.string :as s]
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

(def ^:private catalog-schemas
  "Postgres's own schema-introspection views, not application data. The MCP
  tool surface (beamlynx-desktop's find_tables/complete_query, and their own
  descriptions) explicitly directs an agent to query information_schema.*
  directly for schema discovery -- redacting it would silently defeat that
  and return `xxxxx` for real table/column names, which are not sensitive
  values in the first place. Exempt structurally, the same as an auto-id
  column, not via a configurable rule: there's no legitimate policy under
  which catalog metadata should be masked."
  #{"information_schema" "pg_catalog"})

(defn- catalog-column?
  [state alias source]
  (let [{:keys [schema]} (column-table-info state alias source)]
    (contains? catalog-schemas schema)))

(defn- foreign-key-source-column?
  "Whether `column` is a source column of a detected foreign-key or
  heuristic relation for `table` -- i.e. pine-lang's own join-relation
  index (postgres.clj's index-foreign-keys/index-heuristic-relations,
  both of which populate the same [:table T :refers-to ...] shape) already
  treats it as identifier-shaped, real FK or naming-convention guess alike.
  A lookup against index pine-lang builds anyway, not new indexing."
  [references table column]
  (boolean
   (some (fn [[_target-table entry]] (contains? (:via entry) column))
         (get-in references [:table table :refers-to]))))

(defn- rule-matches?
  "Whether one access-policy rule exempts this column from redaction.
  Unrecognized :type values never match -- fail-closed, and forward-
  compatible with a rule type a different pine-lang version doesn't know
  about yet."
  [state {:keys [column alias source]} rule]
  (case (:type rule)
    "column-type"
    (contains? (set (:allow rule)) (column-db-type state alias column source))

    "foreign-key"
    (let [{:keys [table]} (column-table-info state alias source)]
      (and table (not (s/blank? column))
           (foreign-key-source-column? (:references state) table column)))

    "column-name"
    (and (:suffix rule) (not (s/blank? column)) (s/ends-with? column (:suffix rule)))

    false))

(defn sensitive-column?
  "Whether a SELECT column descriptor should be redacted under `rules`.
  Only meaningful when `rules` is non-empty -- callers gate on that
  themselves (an empty/absent policy means no redaction at all, same as
  before this module existed).

  Auto-generated id columns and Postgres's own catalog views
  (information_schema/pg_catalog -- see catalog-schemas) are always exempt.
  Everything else is redacted unless some rule in `rules` matches --
  including columns pine can't resolve a type or table for at all, such as
  aggregates/computed expressions. Unknown defaults to protected, not
  exposed: a raw aggregate like string_agg(email, ',') would otherwise be a
  one-line bypass of the whole policy."
  [state rules {:keys [alias source auto-id] :as col}]
  (and (not auto-id)
       (not (catalog-column? state alias source))
       (not (some #(rule-matches? state col %) rules))))

(defn- dedupe-by-column-name
  "Keep only the first entry for each :column name, preserving order.
  Needed because references' schema-less [:table table :columns] bucket
  aggregates columns across every schema that has a same-named table (see
  index-columns in postgres.clj) - an unqualified alias whose table name
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
    (when (and table-info (not (:ast table-info)))
      (mapv (fn [{:keys [column]}]
              {:column column :alias current :source {:table table :schema schema}})
            (real-columns state current)))))

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
