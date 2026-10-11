(ns pine.sql.parser
  "Reads one SQL SELECT into plain data for pine.sql.transpile.

  The grammar is src/pine/sql/sql.bnf. This namespace turns its tree into
  maps, with every keyword lowercased and every bare identifier folded to
  lowercase, the way Postgres reads it. A quoted identifier keeps its case.
  Nodes that a refusal may point at carry :at, their offset in the text, so
  a message can name the line.

  A statement the grammar can't read comes back as {:error :error-type}.
  When the text stops at a word Pine has no equivalent for (OFFSET,
  DISTINCT, a function call), the message names it."
  (:require [clojure.java.io :as io]
            [clojure.string :as s]
            [instaparse.core :as insta]))

(def ^:private grammar
  (insta/parser (-> "pine/sql/sql.bnf" io/resource slurp)))

(defn line-of
  "The 1-based line `index` falls on in `text`."
  [text index]
  (inc (count (filter #{\newline} (subs text 0 (min (or index 0) (count text)))))))

(defn- at [node]
  (first (insta/span node)))

;; ----------
;; NORMALIZE
;; ----------

(defn- identifier [[kind value]]
  (case kind
    :bare-identifier (s/lower-case value)
    :quoted-identifier (s/replace value "\"\"" "\"")))

(defn- column [[_ & ids :as node]]
  (let [names (map identifier ids)]
    (cond-> {:name (last names) :at (at node)}
      (next names) (assoc :qualifier (first names)))))

(defn- children
  "The child nodes with tag `tag`."
  [node tag]
  (filter #(and (vector? %) (= tag (first %))) (rest node)))

(defn- child [node tag]
  (first (children node tag)))

(defn- type-name [[_ t]]
  (s/replace (s/lower-case t) #"\s+" " "))

(defn- literal [[kind value :as node]]
  (case kind
    :column {:kind :column :column (column node)}
    :string {:kind :string :value (s/replace (or value "") "''" "'")}
    :number {:kind :number :value value}
    :boolean {:kind :boolean :value (= "true" (s/lower-case (second value)))}
    :null {:kind :null}
    :variable {:kind :variable :name value}))

(defn- operand [[_ inner cast :as node]]
  (if (= :cast-fn (first inner))
    (let [[_ value t] inner]
      (assoc (literal value) :cast (type-name t) :at (at node)))
    (cond-> (assoc (literal inner) :at (at node))
      cast (assoc :cast (type-name (second cast))))))

(declare predicate)

(defn- negated? [node]
  (boolean (child node :negated)))

(defn- operands [node]
  (map operand (children node :operand)))

(defn- predicate [[tag & args :as node]]
  (case tag
    :predicate (predicate (first args))
    :and {:op :and :args (mapv predicate args)}
    :or {:op :or :args (mapv predicate args)}
    :not {:op :not :arg (predicate (first args)) :at (at node)}
    :compare (let [[l [_ op] r] args]
               {:op :compare :operator (if (= op "<>") "!=" op)
                :left (operand l) :right (operand r) :at (at node)})
    :like (let [[l r] (operands node)]
            {:op :like :negated (negated? node)
             :operator (s/lower-case (second (second (child node :like-op))))
             :left l :right r :at (at node)})
    :is-null {:op :is-null :negated (negated? node) :left (first (operands node)) :at (at node)}
    :in (let [[l & vs] (operands node)]
          {:op :in :negated (negated? node) :left l :values (vec vs) :at (at node)})
    :between (let [[l lo hi] (operands node)]
               {:op :between :negated (negated? node) :left l :low lo :high hi :at (at node)})
    :truthy {:op :truthy :column (column (first args)) :at (at node)}))

(defn- table-ref [[_ [_ & ids] alias :as node]]
  (let [names (map identifier ids)]
    (cond-> {:table (last names) :at (at node)}
      (next names) (assoc :schema (first names))
      alias (assoc :alias (identifier (second alias))))))

(defn- join [node]
  (let [[_ [_ & type-nodes] table how] node]
    (cond-> {:type (some-> (ffirst type-nodes) name keyword) :table (table-ref table) :at (at node)}
      (= :on (first how)) (assoc :on (predicate (second how)))
      (= :using (first how)) (assoc :using (mapv identifier (rest how))))))

(defn- select-item [[_ [kind & args :as inner] alias :as node]]
  (cond-> (case kind
            :star {:kind :star}
            :alias-star {:kind :alias-star :qualifier (identifier (first args))}
            :count-star {:kind :count}
            :column {:kind :column :column (column inner)})
    true (assoc :at (at node))
    alias (assoc :as (identifier (second alias)))))

(defn- order-item [[_ [kind value :as target] direction :as node]]
  {:target (case kind
             :column {:kind :column :column (column target)}
             :count-star {:kind :count}
             :position {:kind :position :value value})
   :direction (some-> direction second second s/lower-case keyword)
   :at (at node)})

(defn- normalize [[_ [_ & clauses]]]
  (reduce (fn [acc [tag & args :as node]]
            (case tag
              :select-list (assoc acc :select (mapv select-item args))
              :from (assoc acc :from (table-ref (first args)))
              :join (update acc :joins conj (join node))
              :where (assoc acc :where (predicate (first args)))
              :group (assoc acc :group (mapv column args))
              :order (assoc acc :order (mapv order-item args))
              :limit (assoc acc :limit {:value (Long/parseLong (first args)) :at (at node)})))
          {:joins []}
          clauses))

;; -------
;; ERRORS
;; -------

(def ^:private unsupported-words
  "A word the grammar refuses, and what to say about it. Each is a keyword
  sql.bnf keeps out of identifiers, so a parse stops right on it."
  {"distinct" "DISTINCT isn't in Pine. (group: isn't the same: it adds a count.)"
   "offset" "OFFSET isn't in Pine."
   "fetch" "FETCH isn't in Pine. Use LIMIT."
   "having" "HAVING isn't in Pine."
   "union" "UNION isn't in Pine."
   "intersect" "INTERSECT isn't in Pine."
   "except" "EXCEPT isn't in Pine."
   "full" "A FULL JOIN isn't in Pine. Pine joins inner, left or right."
   "cross" "A CROSS JOIN isn't in Pine. Pine joins two tables on named columns."
   "natural" "A NATURAL JOIN isn't in Pine. Name the columns with ON or USING."
   "with" "WITH isn't in Pine yet."
   "case" "CASE isn't in Pine."
   "exists" "EXISTS isn't in Pine."
   "over" "A window function isn't in Pine."
   "window" "A window function isn't in Pine."
   "nulls" "NULLS FIRST and NULLS LAST aren't in Pine."
   "escape" "LIKE ... ESCAPE isn't in Pine."
   "similar" "SIMILAR TO isn't in Pine. Use like or ilike."
   "lateral" "LATERAL isn't in Pine."
   "all" "ALL isn't in Pine."})

(def ^:private writes
  #{"insert" "update" "delete" "merge" "create" "alter" "drop" "truncate" "grant" "revoke" "copy"})

(defn- hint
  "[message error-type] for a parse that stopped at `index`."
  [text index]
  (let [before (subs text 0 index)
        after (subs text index)
        word (some-> (re-find #"^\s*([A-Za-z_]+)" after) second s/lower-case)
        first-word (some-> (re-find #"^\s*([A-Za-z]+)" text) second s/lower-case)]
    (cond
      (and first-word (writes first-word) (not (re-find #"(?i)^\s*select\b" text)))
      ["Only a SELECT converts to Pine. Write update! and delete! in Pine, where they are scoped by key." "sql-unsupported"]

      (odd? (count (re-seq #"'" (s/replace text "''" ""))))
      ["A string is missing its closing quote." "sql-parse-error"]

      (re-find #"(?i)\bcount\s*\(\s*$" before)
      ["Only COUNT(*) converts. COUNT(column) skips nulls and COUNT(DISTINCT ...) counts values, and Pine's count does neither." "sql-unsupported"]

      (or (re-find #";\s*$" before) (re-find #"^\s*;\s*\S" after))
      ["Only one statement converts at a time." "sql-unsupported"]

      (= word "select")
      ["A subquery isn't in Pine yet." "sql-unsupported"]

      (and word (unsupported-words word))
      [(unsupported-words word) "sql-unsupported"]

      (re-find #"^\s*\(" after)
      (let [f (some-> (re-find #"([A-Za-z_][A-Za-z0-9_]*)\s*$" before) second s/lower-case)]
        (if (#{"sum" "avg" "min" "max" "string_agg" "array_agg"} f)
          [(str (s/upper-case f) "() isn't in Pine. Pine counts only: COUNT(*).") "sql-unsupported"]
          [(str "Functions" (when f (str " like " f "()")) " aren't in Pine.") "sql-unsupported"]))

      (or (re-find #"^\s*\$[0-9]" after) (and (re-find #"\$$" before) (re-find #"^[0-9]" after)))
      ["A positional parameter like $1 isn't in Pine. Write the value, or a named $variable." "sql-unsupported"]

      (re-find #"^\s*\?" after)
      ["A ? placeholder isn't in Pine. Write the value, or a named $variable." "sql-unsupported"]

      (and (re-find #"^\s*," after) (re-find #"(?is)\bfrom\b(?:(?!\b(?:where|join|on|group|order|limit)\b).)*$" before))
      ["A comma between tables isn't in Pine. Join them with JOIN ... ON." "sql-unsupported"]

      (re-find #"^\s*(?:\+|-|\*|/|%|\|\|)" after)
      ["Arithmetic isn't in Pine." "sql-unsupported"]

      :else
      [(let [snippet (s/trim (first (s/split-lines (str after " "))))]
         (if (s/blank? snippet)
           "The SQL ends before the statement does."
           (str "Couldn't read the SQL from `" (subs snippet 0 (min 40 (count snippet))) "`.")))
       "sql-parse-error"])))

(defn parse
  "{:result <statement map>} or {:error <message> :error-type <kind>}.
  error-type is sql-unsupported for SQL that Pine can't say, and
  sql-parse-error for text this reader doesn't understand."
  [text]
  (let [result (grammar text)]
    (if (insta/failure? result)
      (let [index (min (:index result) (count text))
            [message kind] (hint text index)]
        {:error (str "Line " (line-of text index) ": " message) :error-type kind})
      {:result (normalize result)})))

(defn parse-count
  "How many ways the grammar reads `text`. A test asserts this is 1: an
  ambiguous grammar doesn't fail, Instaparse just picks one reading."
  [text]
  (count (insta/parses grammar text)))
