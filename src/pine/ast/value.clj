(ns pine.ast.value
  "Resolve a computed value (see pine.computed) against the state: give each
  column its alias, read a computed name as its value, resolve conditions
  the way where: does, and type a literal by the column beside it.
  See docs/computed-values.md."
  (:require [clojure.string :as s]
            [pine.ast.path :as path]
            [pine.ast.table :as table]
            [pine.ast.where :as where]
            [pine.data-types :as dt]))

(defn- column-info
  "The schema entry of a resolved, uncast column node, or nil."
  [state {:keys [kind alias column path cast]}]
  (when (and (= :column kind) (empty? path) (nil? cast))
    (dt/get-column (:references state) alias column (get (:aliases state) alias))))

(defn- type-literals
  "A literal beside a column takes the column's type, as in where:.
  `coalesce(uuid_col, '...')` and `if c then uuid_col else '...'` would
  otherwise send a varchar that Postgres can't match with a uuid."
  [state nodes]
  (if-let [info (some #(column-info state %) nodes)]
    (mapv (fn [{:keys [kind value] :as node}]
            (if (and (= :literal kind) (#{:string :number :date} (:type value)) (:type info))
              (assoc node :value (dt/convert-value-for-column value info))
              node))
          nodes)
    (vec nodes)))

(declare resolve-node)

(defn- resolve-all
  "[state nodes] for a list of nodes."
  [state nodes aggregates?]
  (reduce (fn [[st acc] n]
            (let [[st n] (resolve-node st n aggregates?)]
              [st (conj acc n)]))
          [state []]
          nodes))

(defn- resolve-column [state node]
  (if-let [ref (path/computed-ref state node)]
    ;; A computed name inside a computed value: its value, as it was
    ;; resolved where it was defined.
    (:computed ref)
    (let [{:keys [alias column path]} (path/resolve-column state (select-keys node [:alias :column :path]))]
      (cond-> {:kind :column :alias alias :column column
               :source (table/column-source state alias column)}
        (seq path) (assoc :path path)
        (:cast node) (assoc :cast (:cast node))))))

(defn resolve-node
  "[state node]: the node resolved. The state changes when a condition uses
  a named result after `in`, whose CTE has to be emitted. `aggregates?` is
  whether the node may be an aggregate: only in group:."
  ([state node] (resolve-node state node false))
  ([state node aggregates?]
   (case (:kind node)
     :column [state (resolve-column state node)]
     :literal [state node]
     :call (let [[state args] (resolve-all state (:args node) false)
                 args (if (= "coalesce" (:fn node)) (type-literals state args) args)]
             [state (assoc node :args args)])
     :condition (let [[state conditions] (where/resolve-conditions state (:conditions node))]
                  [state (assoc node :conditions conditions)])
     :if (let [[state when-node] (resolve-node state (:when node) false)
               [state [then-node else-node]] (resolve-all state (filter some? [(:then node) (:else node)]) false)
               [then-node else-node] (type-literals state (filter some? [then-node else-node]))]
           [state (cond-> (assoc node :when when-node :then then-node)
                    else-node (assoc :else else-node))])
     :aggregate (do
                  (when-not aggregates?
                    (throw (ex-info (str "`" (:fn node) "()` is an aggregate. Use it after `=>` in group:.") {})))
                  (let [[state arg] (if (:arg node) (resolve-node state (:arg node) false) [state nil])
                        [state f] (if (:filter node) (resolve-node state (:filter node) false) [state nil])]
                    [state (assoc node :arg arg :filter f)]))
     (throw (ex-info "Unknown computed value" {:node node})))))

(defn default-name
  "An aggregate's name when `as` doesn't give one: the function, then the
  first column its argument reads, `min_id` for `min(a.id)`. `count` is
  `count`."
  [{fn-name :fn arg :arg}]
  (let [first-column (when arg
                       (letfn [(walk [n]
                                 (case (:kind n)
                                   :column (:column n)
                                   :call (some walk (:args n))
                                   :condition (some (fn [c]
                                                      (let [c (or (first (:or c)) c)]
                                                        (if (vector? c)
                                                          (let [[col] c] (second (:value col)))
                                                          (or (some-> (:computed c) walk) (:column c)))))
                                                    (:conditions n))
                                   :if (or (walk (:when n)) (walk (:then n)))
                                   nil))]
                         (walk arg)))]
    (if (and first-column (not (s/blank? first-column)))
      (str fn-name "_" first-column)
      fn-name)))
