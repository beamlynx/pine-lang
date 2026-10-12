(ns pine.sql.hints
  "What the SQL editor completes from: the connection's tables and columns,
  and after JOIN, the tables that join to the query's. See docs/sql-hints.md."
  (:require [clojure.string :as s]))

(defn default-schema
  "The schema an unqualified table name means: `public` in Postgres. MySQL
  and SQLite have one schema per connection (the database, and `main`)."
  [dialect references]
  (if (= :postgres dialect)
    "public"
    (first (sort (keys (:schema references))))))

(defn schema
  "Every table with its columns in order, by schema and name."
  [dialect references]
  {:dialect (name dialect)
   :default-schema (default-schema dialect references)
   :tables (vec (for [[schema-name {:keys [table]}] (sort-by key (:schema references))
                      [table-name {:keys [columns]}] (sort-by key table)]
                  {:schema schema-name
                   :table table-name
                   :columns (mapv :column columns)}))})

;; ------
;; JOINS
;; ------
;;
;; After `JOIN`, the tables that join to the ones already in the query, with
;; the ON clause written out. The SQL being typed is unfinished, so it is
;; scanned, never parsed: only the text before the cursor, for the tables
;; after FROM and JOIN and their aliases. The joins themselves are Pine's
;; relation hints (foreign keys and names like company_id), asked for
;; through a small Pine expression per table, and written back as SQL.

(def ^:private reserved
  "Words a bare name can't be: Postgres's reserved words and the join words
  around them. A table or alias spelled like one is quoted, and an alias the
  scanner sees is ignored when it is one of these."
  #{"all" "analyse" "analyze" "and" "any" "array" "as" "asc" "asymmetric" "authorization" "between"
    "binary" "both" "case" "cast" "check" "collate" "collation" "column" "concurrently" "constraint"
    "create" "cross" "current_catalog" "current_date" "current_role" "current_schema" "current_time"
    "current_timestamp" "current_user" "default" "deferrable" "desc" "distinct" "do" "else" "end"
    "except" "false" "fetch" "for" "foreign" "freeze" "from" "full" "grant" "group" "having" "ilike"
    "in" "initially" "inner" "intersect" "into" "is" "isnull" "join" "lateral" "leading" "left"
    "like" "limit" "localtime" "localtimestamp" "natural" "not" "notnull" "null" "offset" "on"
    "only" "or" "order" "outer" "overlaps" "placing" "primary" "references" "returning" "right"
    "select" "session_user" "similar" "some" "symmetric" "table" "tablesample" "then" "to"
    "trailing" "true" "union" "unique" "user" "using" "variadic" "verbose" "when" "where" "window"
    "with"})

(defn- blank-out
  "`sql` with comments and string literals replaced by spaces, so nothing
  inside them is read as a table. The length stays the same."
  [sql]
  (s/replace sql #"(?s)--[^\n]*|/\*.*?(\*/|$)|'(?:[^']|'')*('|$)"
             (fn [[m]] (apply str (repeat (count m) \space)))))

(def ^:private ident
  "A name: double-quoted, backquoted, or bare."
  "(?:\"(?:[^\"]|\"\")+\"|`[^`]+`|[A-Za-z_][A-Za-z0-9_$]*)")

(def ^:private table-ref
  (re-pattern (str "(?i)\\b(?:from|join)\\s+(" ident ")(?:\\s*\\.\\s*(" ident "))?"
                   "(?:\\s+(?:as\\s+)?(?!(?:" (s/join "|" (sort reserved)) ")\\b)(" ident "))?")))

(defn- unquote-name
  "[name quoted?] for a name as written."
  [n]
  (cond
    (s/starts-with? n "\"") [(s/replace (subs n 1 (dec (count n))) "\"\"" "\"") true]
    (s/starts-with? n "`") [(subs n 1 (dec (count n))) true]
    :else [n false]))

(defn- db-spelling
  "A bare name as the database spells it: the name itself or its lowercase
  when the database has it, else the one name equal to it ignoring case."
  [n candidates]
  (let [candidates (set candidates)
        matches (filter #(.equalsIgnoreCase ^String n ^String %) candidates)]
    (cond
      (candidates n) n
      (candidates (s/lower-case n)) (s/lower-case n)
      (= 1 (count matches)) (first matches)
      :else (s/lower-case n))))

(defn- scan-tables
  "The tables in `text`, in order: {:schema :table :ref}. :ref is what the
  SQL calls the table, its alias or its name as written."
  [text references]
  (for [[_ a b alias] (re-seq table-ref text)
        :let [[schema-raw table-raw] (if b [a b] [nil a])
              [schema schema-quoted] (some-> schema-raw unquote-name)
              [table table-quoted] (unquote-name table-raw)
              schema (if (and schema (not schema-quoted))
                       (db-spelling schema (keys (:schema references)))
                       schema)
              tables (keys (if schema (get-in references [:schema schema :table]) (:table references)))]]
    {:schema schema
     :table (if table-quoted table (db-spelling table tables))
     :ref (or alias table-raw)}))

(defn- writable-name
  "`n` as SQL that reads it back exactly: bare when it can be, quoted
  otherwise."
  [dialect n]
  (let [bare (if (= :mysql dialect) #"[A-Za-z_][A-Za-z0-9_$]*" #"[a-z_][a-z0-9_]*")]
    (cond
      (and (re-matches bare n) (not (reserved (s/lower-case n)))) n
      (= :mysql dialect) (str "`" (s/replace n "`" "``") "`")
      :else (str "\"" (s/replace n "\"" "\"\"") "\""))))

(defn- new-alias
  "A short alias for `table` that isn't taken: its words' initials, then a
  number."
  [table taken]
  (let [initials (->> (s/split table #"[^A-Za-z0-9]+|(?<=[a-z])(?=[A-Z])")
                      (remove s/blank?)
                      (map first)
                      (apply str)
                      s/lower-case)
        base (if (re-matches #"[a-z][a-z0-9]*" initials) initials "t")]
    (first (remove #(or (taken %) (reserved %))
                   (cons base (map #(str base %) (iterate inc 2)))))))

(defn- pine-name? [n]
  (boolean (re-matches #"[A-Za-z_][A-Za-z0-9_-]*" n)))

(defn join-hints
  "{:joins [...]} for the SQL before `cursor`, when it ends in JOIN and a
  partial name. Each join is {:sql :table :schema :resolution}: :sql is the
  text that goes after JOIN, like `employee e ON e.company_id = c.id`.

  `relations` takes a Pine expression ending in `| ` and returns Pine's
  relation hints there."
  [{:keys [query cursor dialect references relations]}]
  (let [text (blank-out (subs query 0 cursor))
        after-join (re-find #"(?i)\bjoin\s+[^\s]*$" text)]
    (if-not after-join
      {:joins []}
      (let [in-scope (vec (scan-tables (subs text 0 (- (count text) (count after-join))) references))
            default (default-schema dialect references)
            taken (set (map (comp s/lower-case :ref) in-scope))
            write (partial writable-name dialect)
            joins (for [{:keys [schema table ref]} (reverse in-scope)
                        :when (and (pine-name? table) (or (nil? schema) (pine-name? schema)))
                        hint (relations (str (when schema (str schema ".")) table " | "))
                        :when (and (:column hint) (not= "synthetic" (:resolution hint)))
                        :let [alias (new-alias (:table hint) taken)
                              pairs (or (:columns hint) [{:column (:column hint) :related-column (:related-column hint)}])]]
                    {:sql (str (when (and (:schema hint) (not= default (:schema hint))) (str (write (:schema hint)) "."))
                               (write (:table hint)) " " alias " ON "
                               (s/join " AND " (for [{:keys [column related-column]} pairs]
                                                 (str alias "." (write column) " = " ref "." (write related-column)))))
                     :schema (:schema hint)
                     :table (:table hint)
                     :resolution (:resolution hint)})]
        {:joins (vec (distinct joins))}))))
