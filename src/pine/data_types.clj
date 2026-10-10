(ns pine.data-types
  (:require [clojure.string]
            [pine.ast.table :as table]))

(defn string [x]
  {:type :string
   :value x})

(defn number
  "A number written in the expression: `7`, `-2` or `1.5`. A decimal is kept
  exact as a BigDecimal, which a double would round."
  [x]
  {:type :number
   :value (if (clojure.string/includes? x ".")
            (BigDecimal. ^String x)
            (try (Long/parseLong x)
                 (catch NumberFormatException _
                   (throw (ex-info (str x " is too large for a number. Write it as a string: '" x "'.")
                                   {:error-type "parse"})))))})

(defn- parse-temporal
  "A java.sql.Date for 'YYYY-MM-DD', a java.sql.Timestamp when a time
  follows, or nil when `x` isn't a real date. Strict: java.sql.Date/valueOf
  used to roll '2024-02-31' over to 2024-03-02 without a word."
  [x]
  (try
    (if (re-find #"[ T]\d" x)
      (java.sql.Timestamp/valueOf (java.time.LocalDateTime/parse (clojure.string/replace x " " "T")))
      (java.sql.Date/valueOf (java.time.LocalDate/parse x)))
    (catch java.time.format.DateTimeParseException _ nil)))

(defn- invalid-date [x]
  (ex-info (str "'" x "' isn't a valid date. Write it as YYYY-MM-DD, optionally followed by HH:MM or HH:MM:SS.")
           {:error-type "parse" :value x}))

(defn date
  "A literal shaped like a date or date-time. It keeps its source text in
  :text, because the column decides what it is: against a text column it
  stays exactly the text that was written, against a date or time column it
  is the parsed value. Something date-shaped that isn't a real date, like
  '2024-02-31', is a plain string; it is an error only against a date or
  time column."
  [x]
  (if-let [v (parse-temporal x)]
    {:type :date :value v :text x}
    (string x)))

(defn- literal-text
  "The text a literal was written as. A date keeps its source text; the
  parsed value would print as '2024-01-01 10:00:00.0'."
  [value]
  (if (= :date (:type value)) (:text value) (:value value)))

(defn variable
  "A `$name` in the expression: a value supplied with the request (see
  pine.variables). list? marks one used with `in`, which needs a list."
  ([x] {:type :variable :value x})
  ([x list?] (cond-> {:type :variable :value x} list? (assoc :list true))))

(defn named-result
  "A named result (`|= name`) used after `in`: the values its one column
  returns. :column is filled in by pine.ast.where once the named result is
  known."
  [x]
  {:type :named-result :value x})

(defn pine-symbol [x]
  {:type :symbol
   :value x})

(defn jsonb [x]
  {:type :jsonb
   :value x})

(defn uuid [x]
  {:type :uuid
   :value x})

(defn pine-boolean [x]
  {:type :boolean
   :value x})

(defn column
  "Create a column data type. Optionally provide a cast."
  ([column] {:type :column :value [nil column nil]})
  ([column cast] {:type :column :value [nil column cast]})
  ([alias column cast] {:type :column :value [alias column cast]}))

(defn aliased-column
  "Create an aliased column data type. Optionally provide a cast."
  ([alias column] {:type :column :value [alias column nil]})
  ([alias column cast] {:type :column :value [alias column cast]}))

(defn- convert-literal
  [value db-type]
  (case db-type
    "jsonb" (jsonb (literal-text value))
    "json" (jsonb (literal-text value))
    "uuid" (uuid (:value value))
    "boolean" (pine-boolean (:value value))
    "bool" (pine-boolean (:value value))
    ("integer" "int" "int4" "bigint" "int8" "smallint" "int2" "tinyint" "mediumint")
    ;; MySQL's TINYINT/MEDIUMINT are plain integers, not booleans - even
    ;; TINYINT(1), which conventionally holds 0/1. Mapping it to "boolean"
    ;; instead would be a wrong-results bug: `= true` parses as a bare
    ;; :symbol (inlined literally into SQL), but convert-value-to-db-type
    ;; with db-type "boolean" would rewrite it into a :boolean value that
    ;; gets bound as the string "true" - which MySQL coerces to 0, silently
    ;; matching the *false* rows. Leaving it unmapped and inlining the
    ;; literal is simply correct.
    (if (= (:type value) :string)
      ;; Convert string to number if it's actually a number
      (try
        (number (:value value))
        (catch Exception _
          value))
      value)
    ;; bpchar is Postgres's own internal name for CHAR(n)/"character" --
    ;; pg_catalog's pg_type.typname (postgres.clj's get-columns) returns it
    ;; where information_schema.columns would have said "character".
    ;; str: a number given for a text column ($n = 7, say) is compared as
    ;; text. Bound as a number, Postgres refused `varchar = bigint`.
    ;; Only literals are converted; a symbol (true, false) is left alone.
    ("varchar" "character varying" "text" "char" "character" "bpchar" "longtext" "mediumtext" "tinytext")
    (if (#{:string :number :date} (:type value))
      (string (str (literal-text value)))
      value)
    ("date" "timestamp" "timestamptz" "timestamp without time zone" "timestamp with time zone" "datetime")
    (if (= (:type value) :string)
      ;; A string against a date or time column has to be a date.
      ;; '2024-02-31' or 'yesterday' used to be bound as text, which
      ;; Postgres refuses to compare with a timestamp.
      (if-let [v (parse-temporal (:value value))]
        {:type :date :value v :text (:value value)}
        (throw (invalid-date (:value value))))
      value)
    ;; Default: return value as-is
    value))

(defn convert-value-to-db-type
  "Convert a value to the appropriate database type based on the column's schema type.
   Returns the value wrapped in the appropriate data type function."
  [value db-type]
  (if (and (= :symbol (:type value)) (= "NULL" (clojure.string/upper-case (str (:value value)))))
    ;; NULL is NULL whatever the column. Converted, it became the text
    ;; 'NULL' on a text column, or a 'NULL' string bound to a boolean.
    value
    (convert-literal value db-type)))

(defn- columns-for [references table-name schema]
  (if schema
    (get-in references [:schema schema :table table-name :columns])
    (get-in references [:table table-name :columns])))

(defn- raw-column-name
  "Reverse lookup: which raw column does this rename map expose as
  exposed-name? Falls back to exposed-name unchanged when nothing maps to
  it - i.e. it was never renamed to begin with."
  [rename exposed-name]
  (or (some (fn [[raw exposed]] (when (= exposed exposed-name) raw)) rename)
      exposed-name))

(defn get-column-type
  "Get the database type for a column from schema references. Returns the
   database type string or nil if not found. table-info may be a variable's
   alias entry (carrying :ast) - resolved through its real source table(s)
   the same one-hop way join resolution works, since a variable's own
   pre-seeded column list never carried type information to begin with."
  [references alias column-name table-info]
  (if (:ast table-info)
    (some (fn [{:keys [table schema rename]}]
            (let [raw (raw-column-name rename column-name)]
              (some #(when (= (:column %) raw) (:type %))
                    (columns-for references table schema))))
          (table/resolve-table table-info))
    (let [{:keys [table schema]} table-info]
      (some #(when (= (:column %) column-name) (:type %))
            (columns-for references table schema)))))
