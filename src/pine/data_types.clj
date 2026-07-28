(ns pine.data-types
  (:require [pine.ast.table :as table]))

(defn string [x]
  {:type :string
   :value x})

(defn number [x]
  {:type :number
   :value (Long/parseLong x)})

(defn date [x]
  {:type :date
   :value (java.sql.Date/valueOf x)})

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

(defn convert-value-to-db-type
  "Convert a value to the appropriate database type based on the column's schema type.
   Returns the value wrapped in the appropriate data type function."
  [value db-type]
  (case db-type
    "jsonb" (jsonb (:value value))
    "json" (jsonb (:value value))
    "uuid" (uuid (:value value))
    "boolean" (pine-boolean (:value value))
    "bool" (pine-boolean (:value value))
    ("integer" "int" "int4" "bigint" "int8" "smallint" "int2")
    (if (= (:type value) :string)
      ;; Convert string to number if it's actually a number
      (try
        (number (:value value))
        (catch Exception _
          value))
      value)
    ("varchar" "text" "char" "character") (string (:value value))
    ("date" "timestamp" "timestamptz" "timestamp without time zone" "timestamp with time zone")
    (if (= (:type value) :string)
      (try
        (date (:value value))
        (catch Exception _
          value))
      value)
    ;; Default: return value as-is
    value))

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
