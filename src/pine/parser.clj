(ns pine.parser
  "The parser is responsible for generating a parse tree from the bnf and
  normalize the output which is used for the input for generating the ast"
  (:require
   [clojure.core.match :refer [match]]
   [clojure.string :as s]
   [instaparse.core :as insta]
   [pine.data-types :as dt]))

(defmulti -normalize-op
  "Normalize the output of the parser. The first argument is the type of the
  operation"
  first)

;; ----
;; FROM
;; ----

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

              ;; Hint column modifier
              [:table-mod [:hint-column [:symbol column]]] (assoc acc :join-column column)

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

(defn- -normalize-column [column]
  (match column
    [:aliased-column [:column [:symbol c]]]                                            {:column c}
    [:aliased-column [:column [:alias [:symbol a]] [:symbol c]]]                       {:alias a :column c}
    [:aliased-column [:column [:symbol c]] [:alias [:symbol ca]]]                      {:column c :column-alias ca}
    [:aliased-column [:column [:alias [:symbol a]] [:symbol c]] [:alias [:symbol ca]]] {:alias a :column c :column-alias ca}
    [:aliased-column [:alias [:symbol a]] [:star _star]]                                {:alias a :column "" :symbol "*"}
    :else                 (throw (ex-info "Unknown COLUMN operation" {:_ column}))))

(defn normalize-select [payload type]
  (match payload
    [:aliased-columns & columns] {:type type :value (mapv -normalize-column columns)}
    :else                (throw (ex-info (str "Unknown " (name type) " operation") {:_ payload}))))

(defmethod -normalize-op :SELECT [[_ payload]]
  (normalize-select payload :select))

(defmethod -normalize-op :SELECT-PARTIAL [[_ payload]]
  (if (empty? payload)
    {:type :select-partial, :value []}
    (normalize-select payload :select-partial)))

;; ------
;; ORDER
;; ------

(defn- -normalize-order-col [column]
  (match column
    [:order-column [:column [:symbol c]]]   {:column c :direction "DESC"}
    [:order-column [:column [:symbol c]] d] {:column c :direction (s/upper-case d)}
    :else                                   (throw (ex-info "Unknown ORDER operation" {:_ column}))))

(defn -normalize-order [payload type]
  (match payload
    [:order-columns & columns] {:type type :value (mapv -normalize-order-col columns)}
    :else                (throw (ex-info (str "Unknown " (name type) " operation") {:_ payload}))))

(defmethod -normalize-op :ORDER [[_ payload]]
  (-normalize-order payload :order))

(defmethod -normalize-op :ORDER-PARTIAL [[_ payload]]
  (if (empty? payload)
    {:type :order-partial, :value []}
    (-normalize-order payload :order-partial)))

;; -----
;; WHERE
;; -----

(defn- parse-characters [characters] {:type :string :value (apply str characters)})
(defn- parse-strings [[_ & characters]] (parse-characters characters))

(defn- extract-column-info
  [column-pattern]
  (match column-pattern
    [:column [:symbol column]] {:alias nil :column column}
    [:column [:alias [:symbol alias]] [:symbol column]] {:alias alias :column column}
    [:column [:symbol alias] [:symbol column]] {:alias alias :column column}
    :else (throw (ex-info "Unknown column pattern" {:_ column-pattern}))))

(defn- make-column [column-info & [cast-type]]
  (if (:alias column-info)
    (if cast-type
      (dt/aliased-column (:alias column-info) (:column column-info) cast-type)
      (dt/aliased-column (:alias column-info) (:column column-info)))
    (if cast-type
      (dt/column (:column column-info) cast-type)
      (dt/column (:column column-info)))))

(defn- make-condition [column-pattern operator rhs & [cast-type]]
  (let [column-info (extract-column-info column-pattern)
        column-dt (make-column column-info cast-type)
        value-dt (cond
                   (and (vector? rhs) (= (first rhs) :column))
                   (let [rhs-info (extract-column-info rhs)]
                     (make-column rhs-info))
                   :else
                   rhs)]
    {:type :where :value [column-dt operator value-dt]}))

(defn- parse-condition [condition]
  (match condition
    ;; Equals operations
    [:condition column-pattern [:equals] [:number value]]
    (make-condition column-pattern "=" (dt/number value))

    [:condition column-pattern [:equals] [:null]]
    (make-condition column-pattern "IS" (dt/symbol "NULL"))

    [:condition column-pattern [:equals] [:string & characters]]
    (make-condition column-pattern "=" (parse-characters characters))

    [:condition column-pattern [:equals] [:string & characters] [:cast cast-type]]
    (make-condition column-pattern "=" (parse-characters characters) cast-type)

    [:condition column-pattern [:equals] [:boolean b]]
    (make-condition column-pattern "=" (dt/symbol b))

    ;; Column-to-column equals
    [:condition column-pattern [:equals] rhs]
    (make-condition column-pattern "=" rhs)

    ;; Not equals operations
    [:condition column-pattern [:does-not-equal] [:number value]]
    (make-condition column-pattern "!=" (dt/number value))

    [:condition column-pattern [:does-not-equal] [:null]]
    (make-condition column-pattern "IS NOT" (dt/symbol "NULL"))

    [:condition column-pattern [:does-not-equal] [:string & characters]]
    (make-condition column-pattern "!=" (parse-characters characters))

    [:condition column-pattern [:does-not-equal] [:string & characters] [:cast cast-type]]
    (make-condition column-pattern "!=" (parse-characters characters) cast-type)

    [:condition column-pattern [:does-not-equal] [:boolean b]]
    (make-condition column-pattern "!=" (dt/symbol b))

    ;; Column-to-column not equals
    [:condition column-pattern [:does-not-equal] rhs]
    (make-condition column-pattern "!=" rhs)

    ;; IS NULL operations
    [:condition column-pattern [:is] [:null]]
    (make-condition column-pattern "IS" (dt/symbol "NULL"))

    [:condition column-pattern [:is-not] [:null]]
    (make-condition column-pattern "IS NOT" (dt/symbol "NULL"))

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

    ;; IN operations
    [:condition column-pattern [:in] & strings]
    (make-condition column-pattern "IN" (map parse-strings strings))

    [:condition column-pattern [:not-in] & strings]
    (make-condition column-pattern "NOT IN" (map parse-strings strings))

    :else                (throw (ex-info "Unknown condition in WHERE operation"      {:_ condition}))))

(defmethod -normalize-op :WHERE [[_ payload]]
  (match payload
    [:conditions & conditions] (first (map parse-condition conditions))
    :else                (throw (ex-info "Unknown WHERE operation"      {:_ payload}))))

;; -----
;; LIMIT
;; -----

(defmethod -normalize-op :LIMIT [[_ [_ number]]]
  {:type :limit
   :value (Integer/parseInt number)})

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

(defmethod -normalize-op :GROUP [[_ payload]]
  (match payload
    [:group-args [:aliased-columns & columns] [:aggregate-functions & functions]]
    {:type :group
     :value {:columns (mapv -normalize-column columns)
             :functions (map second functions)}}
    :else (throw (ex-info "Unknown GROUP operation" {:_ payload}))))

;; -----
;; DELETE
;; -----

(defmethod -normalize-op :DELETE-ACTION [[_ [_ payload]]]
  (match payload
    [:symbol c] {:type :delete-action :value {:column c}}
    :else (throw (ex-info "Unknown DELETE operation" {:_ payload}))))

;; -----
;; NO-OP
;; -----

(defmethod -normalize-op :DELETE [_]
  {:type :delete :value nil})

;; -----

(def ^:private parser
  (let [dir (System/getProperty "user.dir")
        file (format "%s/src/pine/pine.bnf" dir)
        grammar (slurp file)]
    (insta/parser grammar)))

(defn- normalize-ops [[_ & ops]]
  (mapv (fn [[_ op]] (-normalize-op op)) ops))

(defn parse
  "Parse an expression and return the normalized operations or failure as a string"
  [expression]
  (let [result (parser expression)
        failure? (insta/failure? result)]
    (if failure?
      (let [failure (insta/get-failure result)
            error (with-out-str (println (insta/get-failure result)))]
        {:error error :failure failure})
      {:result (normalize-ops result)})))

(defn parse-or-fail [expression]
  (-> expression parser normalize-ops))
