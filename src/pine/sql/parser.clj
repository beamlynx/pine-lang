(ns pine.sql.parser
  "Reads SQL into plain data for pine.sql.transpile: one SELECT, UPDATE or
  DELETE, with its WITH queries. A script is split on `;` first (see
  split-script), so a statement can be read on its own.

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

(defn- quoted? [[kind]]
  (= :quoted-identifier kind))

(defn- column
  ":quoted marks a quoted column name, which keeps its spelling. A bare one
  may take the database's spelling (pine.sql.transpile)."
  [[_ & ids :as node]]
  (let [names (map identifier ids)]
    (cond-> {:name (last names) :at (at node)}
      (next names) (assoc :qualifier (first names))
      (quoted? (last ids)) (assoc :quoted true))))

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

(declare predicate select-query)

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
    :in-select {:op :in :negated (negated? node) :left (first (operands node))
                :subquery (select-query (child node :select)) :at (at node)}
    :is-distinct (let [[l r] (operands node)]
                   {:op :is-distinct :negated (negated? node) :left l :right r :at (at node)})
    :between (let [[l lo hi] (operands node)]
               {:op :between :negated (negated? node) :left l :low lo :high hi :at (at node)})
    :truthy {:op :truthy :column (column (first args)) :at (at node)}))

(defn- table-ref
  "A table, or a subquery in FROM or JOIN as {:derived <query> :alias}."
  [[tag & args :as node]]
  (if (= tag :derived)
    (let [[q alias] args]
      {:derived (select-query q) :alias (identifier (second alias)) :at (at node)})
    (let [[[_ & ids] alias] args
          names (map identifier ids)]
      (cond-> {:table (last names) :at (at node)}
        (next names) (assoc :schema (first names))
        (quoted? (last ids)) (assoc :quoted true)
        (and (next ids) (quoted? (first ids))) (assoc :schema-quoted true)
        alias (assoc :alias (identifier (second alias)))))))

(defn- join [node]
  (let [[_ [_ & type-nodes] table how] node]
    (cond-> {:type (some-> (ffirst type-nodes) name keyword) :table (table-ref table) :at (at node)}
      (= :on (first how)) (assoc :on (predicate (second how)))
      (= :using (first how)) (assoc :using (mapv (fn [id] (cond-> {:name (identifier id)} (quoted? id) (assoc :quoted true)))
                                                 (rest how))))))

(declare order-item)

(defn- row-number [node]
  {:kind :row-number
   :partition (some->> (child node :partition) rest (mapv column))
   :order (some->> (child node :window-order) rest (mapv order-item))})

(defn- select-item [[_ [kind & args :as inner] alias :as node]]
  (cond-> (case kind
            :star {:kind :star}
            :alias-star {:kind :alias-star :qualifier (identifier (first args))}
            :count-star {:kind :count}
            :row-number (row-number inner)
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

(defn- select-query
  "A SELECT as a map: :select, :from, :joins, :where, :group, :order,
  :limit, and :distinct or :distinct-on."
  [[_ & clauses :as node]]
  (reduce (fn [acc [tag & args :as node]]
            (case tag
              :distinct (assoc acc :distinct {:at (at node)})
              :distinct-on (assoc acc :distinct-on (mapv column args))
              :select-list (assoc acc :select (mapv select-item args))
              :from (assoc acc :from (table-ref (first args)))
              :join (update acc :joins conj (join node))
              :where (assoc acc :where (predicate (first args)))
              :group (assoc acc :group (mapv column args))
              :order (assoc acc :order (mapv order-item args))
              :limit (assoc acc :limit {:value (Long/parseLong (first args)) :at (at node)})))
          {:kind :select :joins [] :at (at node)}
          clauses))

(defn- from-and-joins
  "The FROM (or USING) of an UPDATE or DELETE: {:from :joins}."
  [[_ first-table & joins]]
  {:from (table-ref first-table) :joins (mapv join joins)})

(defn- write-query
  "An UPDATE or DELETE as a map: :kind, :target, :from and :joins (from
  UPDATE's FROM or DELETE's USING), :set and :where."
  [[tag target & clauses :as node]]
  (reduce (fn [acc [ctag & args :as c]]
            (case ctag
              :assignment (update acc :set conj {:column (column (first args)) :value (operand (second args)) :at (at c)})
              (:update-from :delete-using) (merge acc (from-and-joins c))
              :where (assoc acc :where (predicate (first args)))))
          {:kind (if (= tag :update) :update :delete) :target (table-ref target) :set [] :joins [] :at (at node)}
          clauses))

(defn- normalize
  "The statement as a map, with :ctes [{:name :query :at}] from its WITH."
  [[_ & parts]]
  (let [with (first (filter #(= :with (first %)) parts))
        body (first (remove #(= :with (first %)) parts))
        ctes (mapv (fn [[_ id q :as node]] {:name (identifier id) :quoted (quoted? id) :query (select-query q) :at (at node)})
                   (rest with))]
    (assoc (if (= :select (first body)) (select-query body) (write-query body))
           :ctes ctes)))

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
   "recursive" "WITH RECURSIVE isn't in Pine."
   "materialized" "MATERIALIZED isn't in Pine. Remove it: Pine decides how a named result runs."
   "returning" "RETURNING isn't in Pine. update! and delete! report how many rows they changed."
   "case" "CASE isn't in Pine."
   "exists" "EXISTS isn't in Pine."
   "over" "A window function isn't in Pine."
   "window" "A window function isn't in Pine."
   "nulls" "NULLS FIRST and NULLS LAST aren't in Pine."
   "escape" "LIKE ... ESCAPE isn't in Pine."
   "similar" "SIMILAR TO isn't in Pine. Use like or ilike."
   "lateral" "LATERAL isn't in Pine."
   "all" "ALL isn't in Pine."})

(def ^:private other-statements
  "Statements that don't convert: Pine reads, updates and deletes rows."
  #{"insert" "merge" "create" "alter" "drop" "truncate" "grant" "revoke" "copy" "upsert" "replace"})

(defn- re-seq-pos
  "Each match of `re` in `text`, as {:start :end}."
  [re text]
  (let [m (re-matcher re text)]
    (loop [acc []]
      (if (.find m)
        (recur (conj acc {:start (.start m) :end (.end m)}))
        acc))))

(defn- item-start
  "Where the PARTITION BY or ORDER BY item that `inside` ends in starts:
  after the clause's keywords, or after the last comma outside parentheses."
  [inside]
  (let [clause (last (re-seq-pos #"(?i)\b(?:order|partition)\s+by\b" inside))
        from (or (:end clause) 0)]
    (loop [i from depth 0 start from]
      (if (>= i (count inside))
        start
        (let [c (.charAt ^String inside i)]
          (case c
            \( (recur (inc i) (inc depth) start)
            \) (recur (inc i) (dec depth) start)
            \, (recur (inc i) depth (if (zero? depth) (inc i) start))
            (recur (inc i) depth start)))))))

(defn- inside-window
  "When `before` ends inside a ROW_NUMBER() OVER (...), the clause it is in:
  \"PARTITION BY\" or \"ORDER BY\". Otherwise nil."
  [before]
  (when-let [m (last (re-seq-pos #"(?i)\bover\s*\(" before))]
    (let [inside (subs before (:end m))
          depth (reduce (fn [d c] (case c \( (inc d) \) (dec d) d)) 0 inside)]
      (when (>= depth 0)
        (let [order (last (re-seq-pos #"(?i)\border\s+by\b" inside))
              partition (last (re-seq-pos #"(?i)\bpartition\s+by\b" inside))
              clause (cond
                       (and order (or (nil? partition) (> (:start order) (:start partition)))) "ORDER BY"
                       partition "PARTITION BY")]
          (when clause
            {:clause clause :item (subs inside (item-start inside))}))))))

(defn- expression-at
  "The expression `text` starts with, up to a `,` or `)` outside
  parentheses, on one line."
  [text]
  (let [end (loop [i 0 depth 0]
              (if (>= i (count text))
                i
                (let [c (.charAt ^String text i)]
                  (cond
                    (= c \() (recur (inc i) (inc depth))
                    (and (= c \)) (zero? depth)) i
                    (= c \)) (recur (inc i) (dec depth))
                    (and (= c \,) (zero? depth)) i
                    (and (zero? depth) (re-find #"(?i)^\s+order\s+by\b" (subs text i))) i
                    :else (recur (inc i) depth)))))
        expr (s/trim (s/replace (subs text 0 end) #"\s+" " "))
        expr (s/replace expr #"(?i)\s+(asc|desc)$" "")]
    (if (> (count expr) 60) (str (subs expr 0 57) "...") expr)))

(defn- hint
  "[message error-type] for a parse that stopped at `index`."
  [text index]
  (let [before (subs text 0 index)
        after (subs text index)
        word (some-> (re-find #"^\s*([A-Za-z_]+)" after) second s/lower-case)
        first-word (some-> (re-find #"^\s*([A-Za-z]+)" text) second s/lower-case)]
    (cond
      (and first-word (other-statements first-word))
      [(str (s/upper-case first-word) " doesn't convert to Pine. Only SELECT, UPDATE and DELETE do.") "sql-unsupported"]

      (odd? (count (re-seq #"'" (s/replace text "''" ""))))
      ["A string is missing its closing quote." "sql-parse-error"]

      (re-find #"(?i)\bcount\s*\(\s*$" before)
      ["Only COUNT(*) converts. COUNT(column) skips nulls and COUNT(DISTINCT ...) counts values, and Pine's count does neither." "sql-unsupported"]

      (or (re-find #";\s*$" before) (re-find #"^\s*;\s*\S" after))
      ["Only one statement converts at a time." "sql-unsupported"]

      (or (= word "select") (re-find #"(?i)^\s*\(\s*select\b" after))
      ["A subquery here isn't in Pine. One converts in FROM, in JOIN, and after IN." "sql-unsupported"]

      (inside-window before)
      (let [{:keys [clause item]} (inside-window before)]
        [(str clause " " (expression-at (str item after)) " inside ROW_NUMBER() isn't in Pine yet. Pine ranks rows by columns.")
         "sql-unsupported"])

      (and (re-find #"^\s*\(" after) (re-find #"(?is)\bset\b(?:(?!\bwhere\b).)*=\s*[A-Za-z_][A-Za-z0-9_]*\s*$" before))
      (let [f (s/lower-case (second (re-find #"([A-Za-z_][A-Za-z0-9_]*)\s*$" before)))]
        [(str (s/upper-case f) "() isn't in Pine yet. update! writes a value: a string, a number, true, false or null.") "sql-unsupported"])

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
  "{:result <statement map>} or {:error <message> :error-type <kind>} for
  one statement. error-type is sql-unsupported for SQL that Pine can't say,
  and sql-parse-error for text this reader doesn't understand."
  [text]
  (let [result (grammar text)]
    (if (insta/failure? result)
      (let [index (min (:index result) (count text))
            [message kind] (hint text index)]
        {:error (str "Line " (line-of text index) ": " message) :error-type kind})
      {:result (normalize result)})))

;; -------
;; SCRIPTS
;; -------

(defn- statement-ends
  "The offset of each `;` that ends a statement: not one inside a string, a
  quoted name or a comment."
  [^String text]
  (let [n (count text)]
    (loop [i 0 ends []]
      (if (>= i n)
        ends
        (let [c (.charAt text i)
              next-c (when (< (inc i) n) (.charAt text (inc i)))
              skip-to (fn [close]
                        (let [j (.indexOf text ^String close (int (inc i)))]
                          (if (neg? j) n (+ j (count close)))))]
          (cond
            (= c \') (recur (loop [j (inc i)]
                              (cond
                                (>= j n) n
                                (and (= \' (.charAt text j)) (< (inc j) n) (= \' (.charAt text (inc j)))) (recur (+ j 2))
                                (= \' (.charAt text j)) (inc j)
                                :else (recur (inc j))))
                            ends)
            (= c \") (recur (skip-to "\"") ends)
            (= c \`) (recur (skip-to "`") ends)
            (and (= c \-) (= next-c \-)) (recur (skip-to "\n") ends)
            (and (= c \/) (= next-c \*)) (recur (let [j (.indexOf text "*/" (int (+ i 2)))] (if (neg? j) n (+ j 2))) ends)
            (= c \;) (recur (inc i) (conj ends i))
            :else (recur (inc i) ends)))))))

(def ^:private blank-re #"(?s)(?:\s|--[^\n]*|/\*.*?\*/)*")

(defn split-script
  "The statements of a script, split on `;`, as {:start :end}: offsets into
  `text`, without the `;`. Statements that are only spaces and comments are
  left out."
  [text]
  (let [ends (statement-ends text)
        starts (cons 0 (map inc ends))
        stops (concat ends [(count text)])]
    (vec (for [[a b] (map vector starts stops)
               :when (not (re-matches blank-re (subs text a b)))]
           {:start a :end b}))))

(defn transaction-word
  "`begin`, `commit` or `rollback` when `text` is only a statement that
  starts or ends a transaction, else nil."
  [text]
  (when-let [[_ w] (re-matches #"(?is)(?:\s|--[^\n]*|/\*.*?\*/)*(begin|start\s+transaction|commit|end|rollback|abort)(?:\s+(?:work|transaction))?(?:\s|--[^\n]*|/\*.*?\*/)*" text)]
    (case (s/lower-case (first (s/split w #"\s+")))
      ("begin" "start") "begin"
      ("commit" "end") "commit"
      ("rollback" "abort") "rollback")))

(defn statement-kind
  "Whether a statement reads (`:select`), changes rows (`:update`, `:delete`)
  or is something else that writes (`:other-write`), judged from its words.
  A statement that doesn't parse still gets a kind this way."
  [text]
  (let [body (s/replace text #"(?s)--[^\n]*|/\*.*?\*/|'(?:[^']|'')*'" " ")
        first-word (some-> (re-find #"^\s*([A-Za-z]+)" body) second s/lower-case)
        main-word (if (= first-word "with")
                    (some-> (re-find #"(?is)\)\s*(select|update|delete|insert|merge)\b(?!.*\)\s*(?:select|update|delete|insert|merge)\b)" body)
                            second s/lower-case)
                    first-word)]
    (case main-word
      "update" :update
      "delete" :delete
      ("select" "with" nil) :select
      (if (other-statements main-word) :other-write :select))))

(defn mask
  "`text` with everything outside [start, end) blanked to spaces, keeping
  line breaks, so offsets and line numbers still point into the original."
  [^String text start end]
  (apply str (map-indexed (fn [i c] (if (or (< i start) (>= i end)) (if (= c \newline) \newline \space) c)) text)))

(defn parse-count
  "How many ways the grammar reads `text`. A test asserts this is 1: an
  ambiguous grammar doesn't fail, Instaparse just picks one reading."
  [text]
  (count (insta/parses grammar text)))
