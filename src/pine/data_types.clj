(ns pine.data-types)

(defn- t [t x] {:type t
                :value x})
(defn string [x]
  (t :string x))

(defn number [x]
  (t :number (Long/parseLong x)))

(defn date [x]
  (t :date (java.sql.Date/valueOf x)))

#_{:clj-kondo/ignore [:redefined-var]}
(defn symbol [x]
  (t :symbol x))

(defn column
  "Create a column data type. Optionally provide a cast."
  ([column] (t :column [nil column nil]))
  ([column cast] (t :column [nil column cast]))
  ([alias column cast] (t :column [alias column cast])))

(defn aliased-column
  "Create an aliased column data type. Optionally provide a cast."
  ([alias column] (t :column [alias column nil]))
  ([alias column cast] (t :column [alias column cast])))
