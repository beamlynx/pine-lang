(ns pine.describe
  "`describe:`: the table the pipe left off at, as rows. Its columns, the
  tables it joins to and the tables that join to it, from the schema Pine
  read when it connected. Nothing runs on the database. See docs/describe.md."
  (:require [clojure.string :as str]
            [pine.access-policy :as access-policy]
            [pine.ast.hints :as hints]
            [pine.ast.table :as table]
            [pine.db.references :as refs]))

(def header ["kind" "name" "type" "detail" "pine"])

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

(defn- column-rows [state references]
  (let [a (:current state)
        entry (get-in state [:aliases a])
        known (source-columns references entry)
        rules (:access-policy state)
        policy-state (assoc state :references references)]
    (for [{:keys [column]} (hints/generate-all-column-hints state a)
          :let [{:keys [type nullable key?]} (get known column)
                masked (and (seq rules)
                            (access-policy/sensitive-column? policy-state rules {:alias a :column column}))
                detail (str/join ", " (cond-> []
                                        key? (conj "primary key")
                                        ;; SQLite reports an INTEGER PRIMARY KEY as
                                        ;; nullable. A key never is.
                                        (and nullable (not key?)) (conj "nullable")
                                        masked (conj "masked")))]]
      ["column" column (or type "") detail (str a "." column)])))

(defn- resolution-label [resolution]
  (case resolution
    "fk" "foreign key"
    "heuristic" "inferred"
    "synthetic" "same rows"
    "alias" "named with as"
    (str resolution)))

(defn- join-rows
  "The joins Pine would suggest after `| `: each table this one joins to
  (it holds the key, `joins to`) or that joins to it (`joined from`), with
  the Pine to type. The same relations the hints offer, so what
  `describe:` lists is what a pipe can do."
  [state]
  (let [from-alias (:current state)
        {current-schema :schema current-name :table} (get-in state [:aliases from-alias])
        current-table (str (when current-schema (str current-schema ".")) current-name)
        state (assoc state :context from-alias)]
    (for [{:keys [schema table column related-column parent resolution] :as h} (hints/relation-hints state "")
          :let [target (str (when schema (str schema ".")) table)
                [from to] (if parent
                            [(str current-table "." related-column) (str target "." column)]
                            [(str target "." column) (str current-table "." related-column)])]]
      [(if parent "joins to" "joined from")
       target
       ""
       (str (resolution-label resolution) ": " from " -> " to)
       (str "| " (hints/generate-expression h))])))

(defn rows
  "The result of `describe:`, header first, like a query's rows. Refuses a
  pipe with no table to describe."
  [state references]
  (when-not (get-in state [:aliases (:current state)])
    (throw (ex-info "describe: needs a table before it, like `user | describe:`." {:error-type "describe"})))
  (let [read-at (:read-at references)
        state (assoc state :references references)]
    (vec (concat [header]
                 (column-rows state references)
                 (join-rows state)
                 (when read-at
                   [["schema" "read" ""
                     (str "Read at " (.truncatedTo ^java.time.Instant read-at java.time.temporal.ChronoUnit/SECONDS)
                          ". Reindex the connection after the schema changes.")
                     ""]])))))
