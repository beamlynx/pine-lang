(ns pine.describe
  "Two operations that answer from the schema Pine read when it connected,
  for the table the pipe left off at. Nothing runs on the database.

  `describe:` lists the table's columns: one row each, with its type and
  whether it is the key, may be empty or is masked. See docs/describe.md.

  `references:` lists the relations that touch the table, both ways: one
  row per relation, as Pine holds it. See docs/references.md."
  (:require [clojure.string :as str]
            [pine.access-policy :as access-policy]
            [pine.ast.hints :as hints]
            [pine.ast.table :as table]
            [pine.db.references :as refs]
            [pine.similar :as similar]))

(def ^:private op-names {:describe "describe:" :references "references:"})

(defn check-ends-query
  "Refuses an operation after `describe:` or `references:`. Their rows come
  from the schema, not from SQL, so nothing can build on them yet."
  [operation-types]
  (when-let [op (some op-names (butlast operation-types))]
    (throw (ex-info (str op " ends a query for now: nothing can follow it. Remove what comes after it.")
                    {:error-type op}))))

(defn- current-entry
  "The alias entry of the table the pipe left off at. Refuses a pipe with no
  table, naming the operation."
  [state op]
  (or (get-in state [:aliases (:current state)])
      (throw (ex-info (str op " needs a table before it, like `user | " op "`.") {:error-type op}))))

;; describe:

(def describe-header ["column" "type" "key" "nullable" "masked"])

(defn- source-columns
  "{exposed-name {:type :nullable :key?}} for the columns of the real
  table(s) behind `entry`, under the names `entry` exposes them by. A named
  result that selected only some columns exposes only those."
  [references entry]
  (into {}
        (for [{:keys [table schema rename]} (table/resolve-table entry)
              :let [columns (if schema
                              (get-in references [:schema schema :table table :columns])
                              (get-in references [:table table :columns]))
                    key (set (refs/primary-key references schema table))]
              {:keys [column type nullable]} columns
              :let [exposed (table/translate-column rename column)]
              :when exposed]
          [exposed {:type type
                    :nullable (= "YES" (some-> nullable str str/upper-case))
                    :key? (contains? key column)}])))

(defn describe-rows
  "The result of `describe:`, header first, like a query's rows: one row per
  column the current table exposes."
  [state references]
  (let [entry (current-entry state "describe:")
        a (:current state)
        state (assoc state :references references)
        known (source-columns references entry)
        rules (:access-policy state)]
    (vec (cons describe-header
               (for [{:keys [column]} (hints/generate-all-column-hints state a)
                     :let [{:keys [type nullable key?]} (get known column)
                           masked (and (seq rules)
                                       (access-policy/sensitive-column? state rules {:alias a :column column}))]]
                 [column
                  (or type "")
                  (if key? "primary" "")
                  ;; SQLite reports an INTEGER PRIMARY KEY as nullable. A key never is.
                  (if (and nullable (not key?)) "yes" "")
                  (if masked "yes" "")])))))

;; references:

(def references-header ["from" "to" "how" "pine"])

(def ^:private how-names
  "A plain name for each kind of relation the index holds (the :resolution
  pine.db.references writes)."
  {:foreign-key "foreign key"
   :heuristic   "naming"})

(defn- how
  "How Pine knows a relation. A kind not named in how-names shows as its own
  name, so a new way of finding relations appears here with no change."
  [resolution]
  (or (how-names resolution) (some-> resolution name (str/replace "-" " ")) ""))

(defn- side
  "`table.column`, or `table.(a, b)` for a key of several columns, with the
  schema in front when the relation has one."
  [{:keys [schema table]} columns]
  (str (when schema (str schema ".")) table "."
       (if (next columns) (str "(" (str/join ", " columns) ")") (first columns))))

(defn- touching
  "Every relation that touches the real table `source` resolves to, with
  :child? true where the table is the child (it holds the key). A relation
  of a table with itself shows both ways."
  [references {:keys [table schema]}]
  (let [entries (fn [direction]
                  (for [[_ {:keys [via]}] (get-in references [:table table direction])
                        [_ rels] via
                        rel rels
                        :let [own (if (= direction :refers-to) (:child rel) (:parent rel))]
                        :when (and (= table (:table own))
                                   (or (nil? schema) (= schema (:schema own))))]
                    [rel (= direction :refers-to)]))]
    (distinct (concat (entries :refers-to) (entries :referred-by)))))

(defn- pine-by-relation
  "The Pine the hints offer after `| ` from the current table, keyed by the
  table it goes to, the child's first key column, and whether that table is
  the parent: the same three things a relation names."
  [state]
  (into {}
        (for [{:keys [schema table column related-column parent] :as h}
              (hints/relation-hints (assoc state :context (:current state)) "")]
          [[schema table (if parent related-column column) (boolean parent)]
           (str "| " (hints/generate-expression h))])))

(defn references-rows
  "The result of `references:`, header first: one row per relation that
  touches the current table, in both directions.

  Built straight from the relation records in the references map
  (pine.db.references/relation), not from a list of kinds: each row is a
  record's child side (`from`), parent side (`to`), and its :resolution
  (`how`). A new way of finding relations that writes records with a new
  :resolution shows up here as it is, with no change to this format.

  `pine` is what to type after `| ` to follow the relation from the current
  table, as the hints offer it. It is empty where no hint matches, such as
  a named result that doesn't expose the key's column.

  The access policy masks column contents, not names, so every relation is
  listed. Nothing runs on the database."
  [state references]
  (let [entry (current-entry state "references:")
        state (assoc state :references references)
        offered (pine-by-relation state)]
    (vec (cons references-header
               (for [source (table/resolve-table entry)
                     [{:keys [child parent columns resolution]} child?] (touching references source)
                     :let [target (if child? parent child)
                           first-child (:child (first columns))]]
                 [(side child (map :child columns))
                  (side parent (map :parent columns))
                  (how resolution)
                  (or (get offered [(:schema target) (:table target) first-child child?])
                      (get offered [nil (:table target) first-child child?])
                      "")])))))

;; A join that found no relation

(defn- entry-label
  "schema.table, or the table, or the alias of a named result."
  [{:keys [schema table]} a]
  (if schema (str schema "." table) (or table a)))

(defn- same-table? [a b]
  (and (= (:table a) (:table b))
       (or (nil? (:schema a)) (nil? (:schema b)) (= (:schema a) (:schema b)))))

(defn- joins-between
  "[pine how] for each relation from the table behind alias `from` to the
  table behind alias `to`: what to type after `| `, as `references:` shows
  it. A relation with no Pine, such as one a named result can't reach, is
  left out."
  [state references from to]
  (let [state   (assoc state :references references :current from)
        offered (pine-by-relation state)
        targets (table/resolve-table (get-in state [:aliases to]))]
    (distinct
     (for [source (table/resolve-table (get-in state [:aliases from]))
           [{:keys [child parent columns resolution]} child?] (touching references source)
           :let [target (if child? parent child)
                 first-child (:child (first columns))
                 pine (or (get offered [(:schema target) (:table target) first-child child?])
                          (get offered [nil (:table target) first-child child?]))]
           :when (and pine (some #(same-table? target %) targets))]
       [pine (how resolution)]))))

(defn- quoted [names]
  (str/join ", " (map #(str "`" % "`") names)))

(defn unresolved-join-message
  "Why `join` found no relation between its two tables, and the joins that
  do exist between them, with the Pine for each.

  `named` is the columns written after the table, `.user_id`. One that is a
  column of neither table is named, with the closest columns. One that is a
  column but holds no key between the two tables is said to be so."
  [state references {:keys [from to named]}]
  (let [aliases    (:aliases state)
        from-entry (get aliases from)
        to-entry   (get aliases to)
        from-label (entry-label from-entry from)
        to-label   (entry-label to-entry to)
        columns    (concat (keys (source-columns references from-entry))
                           (keys (source-columns references to-entry)))
        missing    (remove (set columns) named)
        joins      (joins-between state references from to)]
    (str/join
     " "
     (remove nil?
             [(str "No relation between `" from-label "` and `" to-label "`.")
              (cond
                (seq missing)
                (str/join " " (for [c missing]
                                (str "`" c "` isn't a column of `" from-label "` or `" to-label "`."
                                     (some->> (similar/did-you-mean c columns) (str " ")))))
                (seq named)
                (str (quoted named) (if (next named) " are columns" " is a column")
                     ", but no key between the two tables uses " (if (next named) "them" "it")
                     ". After a table, `.column` names the column that holds the key, on whichever table has it."))
              (if (seq joins)
                (str "Joins that exist: "
                     (str/join ", " (for [[pine how] joins] (str "`" pine "` (" how ")")))
                     ".")
                (str "Nothing in the schema connects them. Name a column of each table: `" to-label
                     " .x = .y` joins `" from-label "`'s x to `" to-label "`'s y."))]))))
