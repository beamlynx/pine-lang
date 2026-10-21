(ns pine.parser
  "Turns raw Pine text into a normalized operation list.

  Why this layer exists: the BNF grammar produces a raw Instaparse tree
  that's verbose and shape-inconsistent across operation types. Normalizing
  here gives the rest of the system a single, predictable contract:
  a vector of {:type <keyword> :value <data>} maps, one per pipe segment.

  Assignment (|= name) is now a regular pipe operation of type :assign.
  It appears in the operation list like any other op, so the pipeline
  can continue after it: 'company |= x | w: active = true' is valid."
  (:require
   [clojure.core.match :refer [match]]
   [clojure.java.io :as io]
   [clojure.string :as s]
   [instaparse.core :as insta]
   [pine.computed :as computed]
   [pine.data-types :as dt]))

(defmulti -normalize-op
  "Normalize the output of the parser. The first argument is the type of the
  operation"
  first)

;; ----
;; FROM
;; ----

(defn- explicit-pair
  "One `.right = .left` pair, {:left :right}, with :right-cast or :left-cast
  for a cast after either column: `.resourceableId = .id ::text`."
  [[tag & children :as pair]]
  (when-not (= :explicit-pair tag)
    (throw (ex-info "Unknown explicit join columns" {:_ pair})))
  (let [[[right-col right-cast] [left-col left-cast]]
        (reduce (fn [acc [t x]]
                  (case t
                    :hint-column (conj acc [(second x) nil])
                    :cast (update acc (dec (count acc)) assoc 1 x)))
                []
                children)]
    (cond-> {:left left-col :right right-col}
      right-cast (assoc :right-cast right-cast)
      left-cast (assoc :left-cast left-cast))))

(defn- make-table [table-info modifiers]
  (reduce (fn [acc modifier]
            (match modifier
              ;; Direction modifiers
              [:table-mod "parent"] (assoc acc :parent true)
              [:table-mod "child"] (assoc acc :parent false)

              ;; Join type modifiers
              [:table-mod "left"] (assoc acc :join "LEFT")
              [:table-mod "right"] (assoc acc :join "RIGHT")

              ;; Alias modifier
              [:table-mod [:alias [:symbol alias]]] (assoc acc :alias alias)

              ;; Explicit columns modifier: one or more `.right = .left` pairs.
              ;; In "a | b .a_id = .id", the first column of a pair is from the
              ;; right table (b.a_id) and the second from the left (a.id), so
              ;; each pair is stored the other way round from how it is typed.
              [:table-mod [:hint-columns [:explicit-columns & pairs]]]
              (assoc acc :join-column-pairs
                     (mapv explicit-pair pairs))

              ;; Hint column modifier: one or more columns naming which
              ;; relation is meant. They identify it rather than specify it -
              ;; any subset that picks out one relation is enough, and the
              ;; join still uses every column of whatever key that is. See
              ;; docs/joins.md.
              [:table-mod [:hint-columns & hint-columns]]
              (assoc acc :join-columns
                     (mapv (fn [hint-column]
                             (match hint-column
                               [:hint-column [:symbol column]] column
                               :else (throw (ex-info "Unknown join column" {:_ hint-column}))))
                           hint-columns))

              ;; Unknown modifier - ignore
              _ acc))
          table-info
          modifiers))

(defmethod -normalize-op :TABLE [payload]
  (match payload
    ;; Empty table
    [:TABLE [:table-mods]] {:type :table :value {:table ""}}

    ;; Schema.table
    [:TABLE [:symbol schema] [:symbol table] [:table-mods & modifiers]]
    {:type :table :value (make-table {:schema schema :table table} modifiers)}

    ;; Only table
    [:TABLE [:symbol table] [:table-mods & modifiers]]
    {:type :table :value (make-table {:table table} modifiers)}

    :else
    (throw (ex-info "Unknown TABLE operation" {:_ payload}))))

;; -----------------------
;; SELECT / SELECT-PARTIAL
;; -----------------------

(declare parse-characters)

(defn- json-step
  "One step of a JSON path: a key (a string) or an array index (a number)."
  [[tag x]]
  (case tag
    (:json-key :json-quoted-key) (match x
                                   [:symbol k] k
                                   [:string & characters] (:value (parse-characters characters)))
    :json-index (Long/parseLong (second x))))

(defn- column-info
  "{:alias :column} for a [:column ...] node, with :path when it goes into a
  JSON value, and :column-function for a date bucket, `created_at => month`.
  The alias is only what was written before the first dot: pine.ast.path
  decides whether it really is an alias."
  [[tag & children :as node]]
  (when-not (#{:column :bucket-column} tag)
    (throw (ex-info "Unknown column pattern" {:_ node})))
  (let [[head & more] children
        [alias column more] (if (= :alias (first head))
                              [(-> head second second) (-> more first second) (rest more)]
                              [nil (second head) more])
        column-function (some #(when (= :column-function (first %)) (second %)) more)
        steps (remove #(= :column-function (first %)) more)]
    (cond-> {:column column}
      alias (assoc :alias alias)
      (seq steps) (assoc :path (mapv json-step steps))
      column-function (assoc :column-function column-function))))

(declare normalize-value)

(defn- -normalize-column [column]
  (match column
    [:aliased-column [:computed-column node [:alias [:symbol ca]]]] {:computed (normalize-value node) :column-alias ca}
    [:aliased-column [:alias [:symbol a]] [:star _star]] {:alias a :column "" :symbol "*"}
    [:aliased-column col]                                (column-info col)
    [:aliased-column col [:alias [:symbol ca]]]          (assoc (column-info col) :column-alias ca)
    :else                 (throw (ex-info "Unknown COLUMN operation" {:_ column}))))

(defn- parse-partial-alias
  "`c.` is {:alias \"c\" :column \"\"}. A name that stops inside a JSON path,
  `c.data.` or `data.address.`, is the column and its path so far, marked
  :json-partial."
  [[tag & children :as partial-alias]]
  (cond
    (not= tag :partial-alias)
    (throw (ex-info "Unknown partial alias" {:_ partial-alias}))

    (next children)
    (-> (column-info (into [:column] children))
        (update :path #(or % []))
        (assoc :json-partial true))

    :else
    {:alias (-> children first second second) :column ""}))

(defn normalize-select [payload type]
  (match payload
    [:aliased-columns & columns] {:type type :value (mapv -normalize-column columns)}
    :else                (throw (ex-info (str "Unknown " (name type) " operation") {:_ payload}))))

(defmethod -normalize-op :SELECT [[_ payload]]
  (normalize-select payload :select))

(defmethod -normalize-op :SELECT-PARTIAL [operation]
  (let [children (vec (rest operation))
        partial-alias-node (first (filter #(= (first %) :partial-alias) children))
        aliased-columns-node (first (filter #(= (first %) :aliased-columns) children))]
    (cond
      (nil? aliased-columns-node)
      (if partial-alias-node
        {:type :select-partial :value [] :partial-alias (parse-partial-alias partial-alias-node)}
        {:type :select-partial :value []})

      partial-alias-node
      (assoc (normalize-select aliased-columns-node :select-partial)
             :partial-alias (parse-partial-alias partial-alias-node))

      :else
      (normalize-select aliased-columns-node :select-partial))))

;; ------
;; ORDER
;; ------

(defn- -normalize-order-col
  "`a`, `a asc`, `a nulls first` or `a desc nulls last`. Without a
  direction, DESC."
  [column]
  (if (= :order-column (first column))
    (let [[_ col & more] column
          direction (first (filter string? more))
          nulls (some #(when (and (vector? %) (= :nulls-order (first %))) (second %)) more)]
      (cond-> (assoc (column-info col) :direction (if direction (s/upper-case direction) "DESC"))
        nulls (assoc :nulls (s/upper-case nulls))))
    (throw (ex-info "Unknown ORDER operation" {:_ column}))))

(defn -normalize-order [payload type]
  (match payload
    [:order-columns & columns] {:type type :value (mapv -normalize-order-col columns)}
    :else                (throw (ex-info (str "Unknown " (name type) " operation") {:_ payload}))))

(defmethod -normalize-op :ORDER [[_ payload]]
  (-normalize-order payload :order))

(defmethod -normalize-op :ORDER-PARTIAL [operation]
  (let [children (vec (rest operation))
        partial-alias-node (first (filter #(= (first %) :partial-alias) children))
        order-columns-node (first (filter #(= (first %) :order-columns) children))]
    (cond
      (nil? order-columns-node)
      (if partial-alias-node
        {:type :order-partial :value [] :partial-alias (parse-partial-alias partial-alias-node)}
        {:type :order-partial :value []})

      partial-alias-node
      (assoc (-normalize-order order-columns-node :order-partial)
             :partial-alias (parse-partial-alias partial-alias-node))

      :else
      (-normalize-order order-columns-node :order-partial))))

;; -----
;; WHERE
;; -----

;; `string` is one regex token, so `characters` is a single string (or empty
;; for ''). An apostrophe is written doubled inside a literal: 'O''Brien'.
(defn- parse-characters [characters]
  {:type :string :value (s/replace (apply str characters) "''" "'")})
(defn- parse-strings [[_ & characters]] (parse-characters characters))

(defn- extract-column-info
  "{:alias :column}, and :path for a JSON path, of a column in where: or
  update!."
  [column-pattern]
  (merge {:alias nil} (column-info column-pattern)))

(defn- make-column [column-info & [cast-type]]
  (cond-> (if (:alias column-info)
            (if cast-type
              (dt/aliased-column (:alias column-info) (:column column-info) cast-type)
              (dt/aliased-column (:alias column-info) (:column column-info)))
            (if cast-type
              (dt/column (:column column-info) cast-type)
              (dt/column (:column column-info))))
    (:path column-info) (assoc :path (:path column-info))))

(defn- make-condition [column-pattern operator rhs & [cast-type]]
  (let [column-info (extract-column-info column-pattern)
        column-dt (make-column column-info cast-type)
        value-dt (cond
                   (and (vector? rhs) (= (first rhs) :call))
                   {:type :computed :node (normalize-value rhs)}

                   (and (vector? rhs) (= (first rhs) :column))
                   (let [rhs-info (extract-column-info rhs)]
                     (make-column rhs-info))
                   :else
                   rhs)]
    {:type :where :value [column-dt operator value-dt]}))

(def ^:private variable-operators
  "SQL operator for each operator a `$variable` may follow. Not `is`/`is not`:
  those only take null, which a variable can't be."
  {:equals "=" :does-not-equal "!=" :like "LIKE" :not-like "NOT LIKE" :ilike "ILIKE"
   :not-ilike "NOT ILIKE" :greater-than ">" :less-than "<" :greater-than-equal ">="
   :less-than-equal "<="})

(defn- variable-operator [op]
  (or (variable-operators op)
      (throw (ex-info (str "A $variable can't follow `" (name op) "`.") {:operator op}))))

(defn- parse-in-item
  "One value of an `in` list: a string or a number. pine.ast.where types it
  by the column, as it does any other literal."
  [[kind value :as item]]
  (case kind
    :string (parse-strings item)
    :number (dt/number value)))

(defn- in-list-condition
  "`in ('a', 'b')` or `in (1, 2)`, with an optional cast at the end. The cast
  applies to the column, as with every other operator; it is never one of
  the values."
  [column-pattern operator items]
  (let [[cast-type items] (if (= :cast (first (last items)))
                            [(second (last items)) (butlast items)]
                            [nil items])]
    (when-not (every? #(#{:string :number} (first %)) items)
      (throw (ex-info (str "`" (s/lower-case operator) "` takes a list of strings or numbers, a $variable or a named result.") {:_ items})))
    (make-condition column-pattern operator (map parse-in-item items) cast-type)))

(defn- parse-in-condition
  "`in` and `not in` conditions, or nil for any other operator. Kept out of
  parse-condition: core.match nests a class per clause, and with every
  clause in one match the ahead-of-time compiled class names grow past the
  filesystem's 255-character limit (\"File name too long\")."
  [condition]
  (match condition
    ;; A cast after a named result, a $variable or a list applies to the column.
    [:condition column-pattern [:in] [:named-result [:symbol n]]]
    (make-condition column-pattern "IN" (dt/named-result n))

    [:condition column-pattern [:in] [:named-result [:symbol n]] [:cast cast-type]]
    (make-condition column-pattern "IN" (dt/named-result n) cast-type)

    [:condition column-pattern [:not-in] [:named-result [:symbol n]]]
    (make-condition column-pattern "NOT IN" (dt/named-result n))

    [:condition column-pattern [:not-in] [:named-result [:symbol n]] [:cast cast-type]]
    (make-condition column-pattern "NOT IN" (dt/named-result n) cast-type)

    [:condition column-pattern [:in] [:variable n]]
    (make-condition column-pattern "IN" (dt/variable n true))

    [:condition column-pattern [:in] [:variable n] [:cast cast-type]]
    (make-condition column-pattern "IN" (dt/variable n true) cast-type)

    [:condition column-pattern [:not-in] [:variable n]]
    (make-condition column-pattern "NOT IN" (dt/variable n true))

    [:condition column-pattern [:not-in] [:variable n] [:cast cast-type]]
    (make-condition column-pattern "NOT IN" (dt/variable n true) cast-type)

    [:condition column-pattern [:in] & items]
    (in-list-condition column-pattern "IN" items)

    [:condition column-pattern [:not-in] & items]
    (in-list-condition column-pattern "NOT IN" items)

    :else nil))

(defn- call? [node]
  (and (vector? node) (= :call (first node))))

(defn- parse-call-condition
  "A function call on the right of a comparison, `created_at > now()`, or
  nil. Kept out of parse-condition for the same reason as
  parse-in-condition."
  [condition]
  (match condition
    [:condition column-pattern [op] (rhs :guard call?)]
    (make-condition column-pattern (variable-operator op) rhs)

    [:condition column-pattern [op] (rhs :guard call?) [:cast cast-type]]
    (make-condition column-pattern (variable-operator op) rhs cast-type)

    :else nil))

(defn- parse-condition [condition]
  (or
   (parse-call-condition condition)
   (parse-in-condition condition)
   (match condition
     [:condition column-pattern [op] [:variable n]]
     (make-condition column-pattern (variable-operator op) (dt/variable n))

     [:condition column-pattern [op] [:variable n] [:cast cast-type]]
     (make-condition column-pattern (variable-operator op) (dt/variable n) cast-type)

    ;; Equals operations
     [:condition column-pattern [:equals] [:number value]]
     (make-condition column-pattern "=" (dt/number value))

     [:condition column-pattern [:equals] [:null]]
     (make-condition column-pattern "IS" (dt/pine-symbol "NULL"))

     [:condition column-pattern [:equals] [:string & characters]]
     (make-condition column-pattern "=" (parse-characters characters))

     [:condition column-pattern [:equals] [:string & characters] [:cast cast-type]]
     (make-condition column-pattern "=" (parse-characters characters) cast-type)

     [:condition column-pattern [:equals] [:boolean b]]
     (make-condition column-pattern "=" (dt/pine-symbol b))

     [:condition column-pattern [:equals] [:date value]]
     (make-condition column-pattern "=" (dt/date value))

    ;; Column-to-column equals
     [:condition column-pattern [:equals] rhs]
     (make-condition column-pattern "=" rhs)

    ;; Not equals operations
     [:condition column-pattern [:does-not-equal] [:number value]]
     (make-condition column-pattern "!=" (dt/number value))

     [:condition column-pattern [:does-not-equal] [:null]]
     (make-condition column-pattern "IS NOT" (dt/pine-symbol "NULL"))

     [:condition column-pattern [:does-not-equal] [:string & characters]]
     (make-condition column-pattern "!=" (parse-characters characters))

     [:condition column-pattern [:does-not-equal] [:string & characters] [:cast cast-type]]
     (make-condition column-pattern "!=" (parse-characters characters) cast-type)

     [:condition column-pattern [:does-not-equal] [:boolean b]]
     (make-condition column-pattern "!=" (dt/pine-symbol b))

     [:condition column-pattern [:does-not-equal] [:date value]]
     (make-condition column-pattern "!=" (dt/date value))

    ;; Column-to-column not equals
     [:condition column-pattern [:does-not-equal] rhs]
     (make-condition column-pattern "!=" rhs)

    ;; IS NULL operations
     [:condition column-pattern [:is] [:null]]
     (make-condition column-pattern "IS" (dt/pine-symbol "NULL"))

     [:condition column-pattern [:is-not] [:null]]
     (make-condition column-pattern "IS NOT" (dt/pine-symbol "NULL"))

    ;; LIKE operations
     [:condition column-pattern [:like] [:string & characters]]
     (make-condition column-pattern "LIKE" (parse-characters characters))

     [:condition column-pattern [:like] [:string & characters] [:cast cast-type]]
     (make-condition column-pattern "LIKE" (parse-characters characters) cast-type)

    ;; NOT LIKE operations
     [:condition column-pattern [:not-like] [:string & characters]]
     (make-condition column-pattern "NOT LIKE" (parse-characters characters))

     [:condition column-pattern [:not-like] [:string & characters] [:cast cast-type]]
     (make-condition column-pattern "NOT LIKE" (parse-characters characters) cast-type)

    ;; ILIKE operations
     [:condition column-pattern [:ilike] [:string & characters]]
     (make-condition column-pattern "ILIKE" (parse-characters characters))

     [:condition column-pattern [:ilike] [:string & characters] [:cast cast-type]]
     (make-condition column-pattern "ILIKE" (parse-characters characters) cast-type)

    ;; NOT ILIKE operations
     [:condition column-pattern [:not-ilike] [:string & characters]]
     (make-condition column-pattern "NOT ILIKE" (parse-characters characters))

     [:condition column-pattern [:not-ilike] [:string & characters] [:cast cast-type]]
     (make-condition column-pattern "NOT ILIKE" (parse-characters characters) cast-type)

    ;; Greater than operations
     [:condition column-pattern [:greater-than] [:number value]]
     (make-condition column-pattern ">" (dt/number value))

     [:condition column-pattern [:greater-than] [:string & characters]]
     (make-condition column-pattern ">" (parse-characters characters))

     [:condition column-pattern [:greater-than] [:date value]]
     (make-condition column-pattern ">" (dt/date value))

    ;; Less than operations
     [:condition column-pattern [:less-than] [:number value]]
     (make-condition column-pattern "<" (dt/number value))

     [:condition column-pattern [:less-than] [:string & characters]]
     (make-condition column-pattern "<" (parse-characters characters))

     [:condition column-pattern [:less-than] [:date value]]
     (make-condition column-pattern "<" (dt/date value))

    ;; Greater than or equal operations
     [:condition column-pattern [:greater-than-equal] [:number value]]
     (make-condition column-pattern ">=" (dt/number value))

     [:condition column-pattern [:greater-than-equal] [:string & characters]]
     (make-condition column-pattern ">=" (parse-characters characters))

     [:condition column-pattern [:greater-than-equal] [:date value]]
     (make-condition column-pattern ">=" (dt/date value))

    ;; Less than or equal operations
     [:condition column-pattern [:less-than-equal] [:number value]]
     (make-condition column-pattern "<=" (dt/number value))

     [:condition column-pattern [:less-than-equal] [:string & characters]]
     (make-condition column-pattern "<=" (parse-characters characters))

     [:condition column-pattern [:less-than-equal] [:date value]]
     (make-condition column-pattern "<=" (dt/date value))

     :else                (throw (ex-info "Unknown condition in WHERE operation"      {:_ condition})))))

(defmethod -normalize-op :WHERE [[_ payload]]
  (match payload
    [:conditions & conditions]
    (let [values (mapv (comp :value parse-condition) conditions)]
      {:type :where
       :value (if (next values) {:or values} (first values))})
    :else                (throw (ex-info "Unknown WHERE operation"      {:_ payload}))))

(defn- partial-alias? [node]
  (= :partial-alias (first node)))

(defn- parse-partial-condition [partial-condition]
  (match partial-condition
    ;; Alias-dot (e.g. "e."), or a name that stops inside a JSON path
    [:partial-condition (partial-alias :guard partial-alias?)]
    (parse-partial-alias partial-alias)

    ;; Just a column
    [:partial-condition col]
    (column-info col)

    ;; Column + operator (no value)
    [:partial-condition col operator]
    (assoc (column-info col) :operator (first operator))

    :else
    (throw (ex-info "Unknown partial condition" {:_ partial-condition}))))

(defmethod -normalize-op :WHERE-PARTIAL [[_ & children]]
  ;; Up to two children: the complete conditions already joined with `or`,
  ;; then the one still being typed. Either can be missing. Destructuring
  ;; only the first child used to drop the one being typed whenever a
  ;; complete condition came before it.
  (let [[complete partial] (if (= :conditions (ffirst children))
                             [(first children) (second children)]
                             [nil (first children)])]
    {:type :where-partial
     :value {:complete-conditions (mapv parse-condition (rest complete))
             :partial-condition (when partial (parse-partial-condition partial))}}))

;; -----
;; LIMIT
;; -----

(def max-limit
  "The largest `limit:` Pine accepts. A bigger result doesn't fit in the
  server's memory or the results grid, and an out-of-range number used to
  surface as a raw Java NumberFormatException."
  10000)

(defn- parse-limit [number]
  (let [n (try (Long/parseLong number) (catch NumberFormatException _ nil))]
    (when-not (and n (<= n max-limit))
      (throw (ex-info (str "limit: must be between 0 and " max-limit ".")
                      {:error-type "parse" :limit number})))
    n))

(defn- per-column
  "One column after `per`."
  [column]
  (column-info column))

(defn- child [tag children]
  (some #(when (= tag (first %)) %) children))

;; `limit: 1 per customer_id` is {:type :limit :value 1 :per [{:column
;; "customer_id"}]}. :value stays the number, as for a plain limit:, so
;; anything that reads it doesn't need to know about `per`.
(defmethod -normalize-op :LIMIT [[_ [_ number] columns]]
  (cond-> {:type :limit
           :value (parse-limit number)}
    columns (assoc :per (mapv per-column (rest columns)))))

;; While the columns after `per` are typed: `l: 1 per `, `l: 1 per a, ` or
;; `l: 1 per c.`. It keeps the columns typed so far, as order-partial does,
;; so hints can leave them out.
(defmethod -normalize-op :LIMIT-PARTIAL [[_ [_ number] & children]]
  (let [columns (child :columns children)
        partial-alias (child :partial-alias children)]
    (cond-> {:type :limit-partial
             :value (parse-limit number)
             :per (mapv per-column (rest columns))}
      partial-alias (assoc :partial-alias (parse-partial-alias partial-alias)))))

;; -----
;; FROM
;; -----

(defmethod -normalize-op :FROM [[_ payload]]
  (match payload
    [:alias [:symbol c]] {:type :from :value {:alias c}}
    :else (throw (ex-info "Unknown FROM operation" {:_ payload}))))

;; -----
;; COUNT
;; -----

(defmethod -normalize-op :COUNT [[_ [_ _payload]]]
  {:type :count :value {:column "*"}})

;; -----
;; GROUP
;; -----

(defn- -normalize-group-column [column]
  (column-info column))

(declare normalize-conditions)

(defn- aggregate-of
  "[fn-name arg-nodes] for a parsed aggregate: `count`, a call `min(a.id)`,
  or an arrow `a.id => min`, whose value before the last step is the
  argument. Nil when it isn't a function at all."
  [[tag & children :as node]]
  (case tag
    :value-column (let [info (column-info (first children))
                        bare? (and (not (:alias info)) (not (:path info)) (not (child :cast children)))
                        f (get computed/functions (s/lower-case (:column info)))]
                    (cond
                      (and bare? (= "count" (s/lower-case (:column info)))) ["count" []]
                      (and bare? (:aggregate f))
                      (throw (ex-info (str "`" (:column info) "` needs a value: `x => " (:column info)
                                           "`, like `total => " (:column info) "`.")
                                      {:error-type "arity" :function (:column info)}))))
    :call (let [[[_ fn-name] & args] children]
            (when-let [cast (some #(when (= :cast (first %)) (second %)) args)]
              (throw (ex-info (str "A cast can't follow an aggregate. Cast its argument: `x::" cast " => " fn-name "`.")
                              {:error-type "parse"})))
            [fn-name (vec args)])
    :arrow-value (let [base (vec (butlast children))
                       step (last children)
                       arg (if (= 1 (count base)) (first base) (into [:arrow-value] base))]
                   (case (first step)
                     :function-name [(-> step second second) [arg]]
                     :call (let [[_ [_ fn-name] & args] step]
                             (when-let [cast (some #(when (= :cast (first %)) (second %)) args)]
                               (throw (ex-info (str "A cast can't follow an aggregate. Cast its argument: `x::" cast " => " fn-name "`.")
                                               {:error-type "parse"})))
                             [fn-name (into [arg] args)])))
    :paren-value (aggregate-of (first children))
    nil))

(defn- normalize-aggregate
  "One aggregate after `with`: {:kind :aggregate :fn :arg :filter}, with
  :column-alias when it is named with `as`. `count` takes no argument."
  [[_ head & more]]
  (let [filter-node (some #(when (= :conditions (first %)) %) more)
        alias-node (some #(when (= :alias (first %)) %) more)
        [fn-name args] (or (aggregate-of head)
                           (throw (ex-info (str "After `with` in group: come aggregates, like `count` or `total => max`. "
                                                "There are " (s/join ", " (computed/function-names true)) ".")
                                           {:error-type "not-an-aggregate"})))]
    (computed/check-call fn-name (count args) true)
    (cond-> {:kind :aggregate
             :fn (s/lower-case fn-name)
             :arg (some-> (first args) normalize-value)
             :filter (some-> filter-node normalize-conditions)}
      alias-node (assoc :column-alias (-> alias-node second second)))))

(defmethod -normalize-op :GROUP [[_ payload]]
  (match payload
    [:group-args [:columns & columns] [:aggregate-functions & functions]]
    {:type :group
     :value {:columns (mapv -normalize-group-column columns)
             :functions (mapv normalize-aggregate functions)}}
    [:group-args [:columns & columns]]
    {:type :group
     :value {:columns (mapv -normalize-group-column columns)
             :functions [{:kind :aggregate :fn "count" :arg nil :filter nil}]}}
    [:legacy-group-args & _]
    (throw (ex-info (str "In group:, `with` divides the keys from the aggregates now, and `=>` applies a function. "
                         "Write `group: status with count`, or just `group: status`, which counts each group.")
                    {:error-type "parse"}))
    :else (throw (ex-info "Unknown GROUP operation" {:_ payload}))))

;; -----
;; DELETE
;; -----

(defmethod -normalize-op :DELETE-ACTION [[_ & hint-columns]]
  ;; One column or several: `delete! .id`, `delete! .case_id, .search_id`.
  ;; Several are needed when the rows to remove are identified by more than
  ;; one column - a table whose key is composite has no single column that
  ;; picks out a row on its own, and deleting on one of them at a time takes
  ;; rows belonging to other records with it.
  {:type :delete-action
   :value {:columns (mapv (fn [hint-column]
                            (match hint-column
                              [:hint-column [:symbol c]] c
                              :else (throw (ex-info "Unknown DELETE operation" {:_ hint-column}))))
                          hint-columns)}})

;; -----
;; UPDATE
;; -----

(defn- parse-update-assignment [assignment]
  (match assignment
    [:update-assignment column-pattern [:string & characters]]
    {:column (extract-column-info column-pattern) :value (parse-characters characters)}

    [:update-assignment column-pattern [:number value]]
    {:column (extract-column-info column-pattern) :value (dt/number value)}

    [:update-assignment column-pattern [:boolean b]]
    {:column (extract-column-info column-pattern) :value (dt/pine-symbol b)}

    [:update-assignment column-pattern [:null]]
    {:column (extract-column-info column-pattern) :value (dt/pine-symbol "NULL")}

    [:update-assignment column-pattern [:date value]]
    {:column (extract-column-info column-pattern) :value (dt/date value)}

    [:update-assignment column-pattern [:variable n]]
    {:column (extract-column-info column-pattern) :value (dt/variable n)}

    [:update-assignment column-pattern (call :guard call?)]
    {:column (extract-column-info column-pattern) :value {:type :computed :node (normalize-value call)}}

    ;; Column-to-column assignment: `update! name = country`. Built as a
    ;; typed column, like a column on the right of a where: comparison. It
    ;; used to be a bare {:alias :column} map with no :type, which eval
    ;; rendered as a `?` bound to NULL.
    [:update-assignment column-pattern rhs]
    {:column (extract-column-info column-pattern) :value (make-column (extract-column-info rhs))}

    :else (throw (ex-info "Unknown update assignment" {:_ assignment}))))

(defmethod -normalize-op :UPDATE-ACTION [[_ payload]]
  (match payload
    [:update-assignments & assignments]
    {:type :update-action :value {:assignments (mapv parse-update-assignment assignments)}}
    :else (throw (ex-info "Unknown UPDATE-ACTION operation" {:_ payload}))))

(defmethod -normalize-op :UPDATE-PARTIAL [operation]
  (match (vec (rest operation))
    []
    {:type :update-partial :value {:assignments [] :partial-column nil}}

    [[:partial-update-column (partial-alias :guard partial-alias?)]]
    {:type :update-partial :value {:assignments []
                                   :partial-column (parse-partial-alias partial-alias)}}

    [[:partial-update-column column-pattern]]
    {:type :update-partial :value {:assignments []
                                   :partial-column (extract-column-info column-pattern)}}

    [[:update-assignments & assignments]]
    {:type :update-partial :value {:assignments (mapv parse-update-assignment assignments)
                                   :partial-column nil}}

    [[:update-assignments & assignments] [:partial-update-column (partial-alias :guard partial-alias?)]]
    {:type :update-partial :value {:assignments (mapv parse-update-assignment assignments)
                                   :partial-column (parse-partial-alias partial-alias)}}

    [[:update-assignments & assignments] [:partial-update-column column-pattern]]
    {:type :update-partial :value {:assignments (mapv parse-update-assignment assignments)
                                   :partial-column (extract-column-info column-pattern)}}

    :else (throw (ex-info "Unknown UPDATE-PARTIAL operation" {:_ operation}))))

;; ---------------
;; COMPUTED VALUES
;; ---------------
;;
;; A tree of pine.computed nodes. Conditions stay [column operator value]
;; triples, as parse-condition gives them; pine.ast.value resolves them the
;; way where: does.

(defn- normalize-conditions
  "A [:conditions ...] node as a :condition node."
  [[_ & conditions]]
  {:kind :condition :conditions (mapv (comp :value parse-condition) conditions)})

(defn- normalize-literal [[_ [kind value :as literal]]]
  (case kind
    :string (parse-strings literal)
    :date (dt/date value)
    :number (dt/number value)
    :boolean (dt/pine-symbol value)
    :null (dt/pine-symbol "NULL")))

(defn- with-cast [node children]
  (if-let [cast (some #(when (= :cast (first %)) (second %)) children)]
    (assoc node :cast cast)
    node))

(defn normalize-value
  "A parsed computed value (computed-column's value, a call's argument, ...)
  as a pine.computed node."
  [[tag & children :as node]]
  (case tag
    :paren-value (normalize-value (first children))
    :condition-value (normalize-conditions (first children))
    :if-value (let [[condition then else] children]
                (cond-> {:kind :if :when (normalize-conditions condition) :then (normalize-value then)}
                  else (assoc :else (normalize-value else))))
    :call (let [[[_ fn-name] & args] children
                args (remove #(= :cast (first %)) args)]
            (computed/check-call fn-name (count args) false)
            (with-cast {:kind :call :fn (s/lower-case fn-name) :args (mapv normalize-value args)} children))
    :arrow-value (reduce (fn [node [tag & step]]
                           (case tag
                             :function-name (let [fn-name (-> step first second)]
                                              (computed/check-call fn-name 1 false)
                                              {:kind :call :fn (s/lower-case fn-name) :args [node]})
                             :call (let [[[_ fn-name] & args] step
                                         args (remove #(= :cast (first %)) args)]
                                     (computed/check-call fn-name (inc (count args)) false)
                                     (with-cast {:kind :call :fn (s/lower-case fn-name)
                                                 :args (into [node] (map normalize-value args))}
                                       step))))
                         (normalize-value (first children))
                         (rest children))
    (:value-column :cast-column) (with-cast (merge {:kind :column :alias nil} (extract-column-info (first children))) children)
    :literal {:kind :literal :value (normalize-literal node)}
    :variable {:kind :literal :value (dt/variable (first children))}
    (throw (ex-info "Unknown computed value" {:_ node}))))

;; -----
;; PATHS
;; -----

(defmethod -normalize-op :PATHS [payload]
  (match payload
    ;; Empty target: "? "
    [:PATHS] {:type :paths :value {:table ""}}

    ;; Schema.table
    [:PATHS [:symbol schema] [:symbol table]]
    {:type :paths :value {:table table :schema schema}}

    ;; Only table
    [:PATHS [:symbol table]]
    {:type :paths :value {:table table}}

    :else
    (throw (ex-info "Unknown PATHS operation" {:_ payload}))))

;; -----
;; ASSIGN
;; -----

(defmethod -normalize-op :ASSIGN [[_ [_ varname]]]
  {:type :assign :value varname})

;; -----

(def ^:private parser
  (let [grammar (-> "pine/pine.bnf" io/resource slurp)]
    (insta/parser grammar)))

(defn- normalize-ops [[_ & nodes]]
  (mapv (fn [[_ op]] (-normalize-op op))
        (filter #(= (first %) :OPERATION) nodes)))

;; ---------------------------------------------------------------------------
;; Readable parse errors
;; ---------------------------------------------------------------------------
;;
;; Instaparse reports a position and every token the grammar would have
;; accepted there, regex literals included. Correct, but it rarely says what
;; went wrong. For the common mistakes a sentence that does is put in front;
;; the raw report stays after it (beamlynx-desktop's MCP formatter reads its
;; "Parse error at line" and "Expected one of" lines).

(def ^:private operation-names
  ["select" "where" "limit" "from" "order" "group" "count" "delete!" "update!"])

(defn- edit-distance [a b]
  (let [n (count b)]
    (peek
     (reduce (fn [prev [i ca]]
               (reduce (fn [row [j cb]]
                         (conj row (min (inc (peek row))
                                        (inc (nth prev (inc j)))
                                        (+ (nth prev j) (if (= ca cb) 0 1)))))
                       [(inc i)]
                       (map-indexed vector b)))
             (vec (range (inc n)))
             (map-indexed vector a)))))

(defn- closest-operation [word]
  (let [word (s/lower-case word)
        [best distance] (apply min-key second (map (juxt identity #(edit-distance word %)) operation-names))]
    (when (<= distance 2) best)))

(defn- parse-error-hint
  "A sentence naming the likely mistake, or nil."
  [expression {:keys [index]}]
  (let [index  (min (or index 0) (count expression))
        before (subs expression 0 index)
        after  (subs expression index)
        word   (second (re-find #"([A-Za-z]+!?)\s*$" before))]
    (cond
      (re-find #"(?is)^\s*select\b.*\bfrom\b" expression)
      "This looks like SQL. Paste it in the SQL panel, and it converts to Pine as you type."

      (odd? (count (re-seq #"'" (s/replace expression "''" ""))))
      "A string is missing its closing quote. To put an apostrophe inside a string, write it twice: 'O''Brien'."

      ;; `s: coalesce(a, b)` with no `as`: a computed value has to be named.
      (and (re-find #"(?:select:|s:)[^|]*$" before) (re-find #"[)'0-9]\s*$" before) (re-find #"^\s*(?:,|\||$)" after))
      "A computed value in select: needs a name: `coalesce(a, b) as name`."

;; `type = 'x' => any`: the arrow would apply to 'x', not to the condition.
      (and (re-find #"^\s*=>" after) (re-find #"(?:[=<>]|\b(?:like|ilike|is|in)\b)[^,|()]*$" before))
      "A condition before `=>` goes in parentheses, `(type = 'x') => any`, or is written as a call: `any(type = 'x')`."

      (and (re-find #"(?:where:|w:)[^|]*$" before) (re-find #"^\s*(?:,|and\b)" after))
      "Join conditions in one where: with `or`. To require several, add a where: for each: `| where: a = 1 | where: b = 2`."

      (and word (s/starts-with? after ":") (not (some #{(s/lower-case word)} operation-names)))
      (if-let [op (closest-operation word)]
        (str "Unknown operation `" word ":`. Did you mean `" op ":`?")
        (str "Unknown operation `" word ":`."))

      (and word (some #{(s/lower-case word)} ["select" "where" "limit" "from" "order" "group"])
           (re-find #"^\s*[^:\s|]" after))
      (str "`" word "` needs a colon: `" (s/lower-case word) ":`."))))

(defn parse
  "Parse an expression and return the normalized operations or failure as a string"
  [expression]
  (let [result (parser expression)
        failure? (insta/failure? result)]
    (if failure?
      (let [failure (insta/get-failure result)
            raw (with-out-str (println failure))
            hint (parse-error-hint expression failure)]
        {:error (if hint (str hint "\n\n" raw) raw) :failure failure})
      {:result (normalize-ops result)})))

(defn parse-or-fail [expression]
  (-> expression parser normalize-ops))

;; ---------
;; DOC COMMENT
;; ---------
;;
;; A comment at the very top of an expression documents what the query is
;; for. The grammar already accepts comments anywhere whitespace is legal
;; (see pine.bnf's `ws`), but every rule hides `ws`, so the text never
;; reaches the parse tree - by the time anything downstream sees the
;; expression, the comment is gone.
;;
;; Reading it straight off the raw string rather than adding a DOC rule to
;; the grammar is deliberate. The grammar is already correct about comments;
;; it only discards them. A new top-level rule would have to compete with
;; OPERATION for the same leading position in a grammar that every single
;; operation depends on, and the whole payoff would be a few characters this
;; scan reads just as exactly.
;;
;; Only the leading comment is a doc. Comments between operations stay
;; whitespace, as they are today - attaching one to the operation it sits
;; next to needs an anchor the expression doesn't currently have.

(def ^:private leading-block-comment
  ;; (?s) so `.` spans newlines; non-greedy so the first `*/` closes it.
  #"(?s)\A\s*/\*(.*?)\*/")

(def ^:private leading-line-comments
  ;; A run of consecutive `--` lines. A genuinely blank line ends the run; a
  ;; bare `--` (the usual way to space out paragraphs) does not.
  #"\A\s*(?:[ \t]*--[^\r\n]*(?:\r?\n|\z))+")

(defn- strip-marker
  "Strip a per-line comment marker, plus the single space that conventionally
  follows it. `--` on every line for a line-comment doc; the optional leading
  `*` of a javadoc-style block, but only when every non-blank line has one -
  otherwise a line legitimately starting with `*` would lose a character."
  [marker lines]
  (let [re (re-pattern (str "\\A[ \\t]*" marker "[ \\t]?"))
        marked? (fn [line] (or (s/blank? line) (re-find re line)))]
    (if (every? marked? lines)
      (mapv #(s/replace % re "") lines)
      lines)))

(defn- dedent
  "Remove the indentation every non-blank line shares."
  [lines]
  (let [indents (->> lines
                     (remove s/blank?)
                     (map #(count (re-find #"\A[ \t]*" %))))]
    (if (seq indents)
      (let [n (apply min indents)]
        (mapv #(if (s/blank? %) "" (subs % n)) lines))
      lines)))

(defn- clean-doc [text marker]
  (->> (s/split-lines text)
       (mapv s/trimr)
       (strip-marker marker)
       dedent
       (drop-while s/blank?)
       reverse
       (drop-while s/blank?)
       reverse
       (s/join "\n")))

(defn extract-doc
  "The doc comment at the top of an expression, or nil when there isn't one.

  A leading `/* ... */` block, or a run of consecutive leading `--` lines.
  Returns {:text <cleaned for display> :end <offset just past the comment>}.
  `:end` is what prettify needs to copy the comment through verbatim; `:text`
  has the delimiters, per-line markers and shared indentation removed, so a
  caller can render it as prose. A comment with nothing but whitespace in it
  is not a doc."
  [expression]
  (when (string? expression)
    (let [[matched text marker]
          (if-let [[m inner] (re-find leading-block-comment expression)]
            [m inner "\\*"]
            (when-let [m (re-find leading-line-comments expression)]
              [m m "--"]))]
      (when matched
        (let [cleaned (clean-doc text marker)]
          (when-not (s/blank? cleaned)
            {:text cleaned :end (count matched)}))))))

(defn prettify
  "Prettify a Pine expression by formatting each operation on its own line.
   Uses the parser to correctly handle pipes inside string values.
   Returns {:error ...} on parse failure so the caller can keep the original string.
   On success returns {:result <prettified string>
                       :operations [{:text <op text> :start <n> :end <n>} ...]}
   where :start/:end are character offsets in the original expression.

   A doc comment at the top is copied through verbatim onto its own lines.
   It has to be: prettify rebuilds the expression out of the operations'
   own spans, and a leading comment is in none of them - so without this it
   is deleted the first time anything prettifies, which in canvas mode is
   the very next gesture. Comments between operations are still dropped,
   same as before."
  [expression]
  (let [result (parser expression)]
    (if (insta/failure? result)
      {:error (with-out-str (println (insta/get-failure result)))}
      (let [operations (rest result)
            op-infos (mapv (fn [op]
                             (let [[start end] (insta/span op)
                                   expr (s/trim (subs expression start end))]
                               {:expression expr
                                :start start
                                :end end}))
                           operations)
            body (s/join "\n | " (map :expression op-infos))
            doc (extract-doc expression)]
        {:result (if doc
                   (str (s/trim (subs expression 0 (:end doc))) "\n" body)
                   body)
         :operations op-infos}))))
