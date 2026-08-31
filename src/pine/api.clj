(ns pine.api
  "HTTP API layer: routes, request parsing, and multi-expression evaluation.

  Why multi-expression evaluation exists: Pine expressions are designed to be
  composed across blank-line-separated blocks. Each block can assign its result
  to a variable (|= name) which subsequent blocks use as a CTE. This file
  threads variables between expressions so each one sees what earlier ones
  produced. The last expression's SQL is the one actually returned or executed."
  (:require
   [cheshire.generate :refer [add-encoder encode-str]]
   [clojure.string :as str]
   [compojure.core :refer [defroutes DELETE GET POST]]
   [compojure.route :as route]
   [pine.access-policy :as access-policy]
   [pine.ast.main :as ast]
   [pine.db.connections :as connections] ;; Encode arrays and json results in API responses
   [pine.db.main :as db]
   [pine.eval :as eval]
   [pine.parser :as parser]
   [pine.version :as v]
   [ring.middleware.cors :refer [wrap-cors]]
   [ring.middleware.defaults :refer [api-defaults wrap-defaults]]
   [ring.middleware.json :refer [wrap-json-params wrap-json-response]]
   [ring.util.response :refer [response]])
  (:import
   [java.util TimeZone]))

;; Set default timezone to UTC
(TimeZone/setDefault (TimeZone/getTimeZone "UTC"))

;; array/json encoding
(add-encoder org.postgresql.util.PGobject encode-str)
(add-encoder org.postgresql.jdbc.PgArray encode-str)

(def version v/version)

(defn- generate-state
  ([expression]
   (generate-state expression nil nil {}))
  ([expression cursor]
   (generate-state expression cursor nil {}))
  ([expression cursor connection-id]
   (generate-state expression cursor connection-id {}))
  ([expression cursor connection-id variables]
   (generate-state expression cursor connection-id variables []))
  ([expression cursor connection-id variables access-policy]
   (let [{:keys [result error]} (->> expression parser/parse)
         conn-id (or connection-id @db/connection-id)]
     (if result
       {:result (ast/generate result conn-id expression cursor variables access-policy)}
       {:error-type "parse"
        :error error}))))

(defn- evaluate-expressions
  "Evaluate a sequence of pine expressions, threading variables from |= assignments
  into subsequent expressions. Returns {:last-state <state> :error <msg>}.

  Why pending-assignments: |= is now a mid-pipeline op that snapshots state at the
  point of assignment. An expression can assign multiple variables before its last op
  determines the final SQL. All assignments from one expression become available as
  CTE variables to the next."
  ([expressions connection-id]
   (evaluate-expressions expressions connection-id []))
  ([expressions connection-id access-policy]
   (reduce (fn [{:keys [variables]} expression]
             (let [{:keys [result error]} (generate-state expression nil connection-id variables access-policy)]
               (if error
                 (reduced {:error error})
                 {:variables (merge variables (:pending-assignments result))
                  :last-state result})))
           {:variables {} :last-state nil}
           expressions)))

(defn- trim-pipes [s]
  (-> s
      (str/trim)
      (str/replace #"^\|\s*|\s*\|$" "")
      (str/trim)))

(defn- prune-table
  "Prune a table entry down to what the frontend's Table type (client.ts) uses.
  Critical: a variable-backed table entry carries a full :ast (the variable's own
  var-ast) for the query builder's CTE generation — left unpruned, it recursively
  re-embeds that variable's entire state (and, transitively, everything it wraps in
  turn) inside every table list that references it."
  [table]
  (select-keys table [:schema :table :alias]))

(defn- prune-var-ast
  "Prune a variable/pending-assignment snapshot down to what the frontend actually
  uses (VariableAst in client.ts). Critical: a raw snapshot still carries :variables
  and :references from pre-handle/post-handle — left unpruned, each additional
  chained |= block would re-embed every earlier block's full snapshot inside the new
  one, growing the response payload superlinearly instead of linearly. Its own
  :tables/:selected-tables entries need the same per-table pruning (prune-table) for
  a variable-of-variable chain, or the same recursive embedding reappears one level
  down."
  [var-ast]
  (-> (select-keys var-ast [:tables :selected-tables :joins :columns])
      (update :tables #(mapv prune-table %))
      (update :selected-tables #(mapv prune-table %))))

(defn- prune-ast
  "Prune a generated state down to the :ast value returned to the frontend."
  [state]
  (-> (select-keys state [:hints :selected-tables :joins :context :current :operation :columns :order :where :group :prettified :ranges :assign])
      (update :selected-tables #(mapv prune-table %))
      (assoc :variables
             (into {} (for [[k v] (:variables state)] [k (prune-var-ast v)])))
      (assoc :pending-assignments
             (into {} (for [[k v] (:pending-assignments state)] [k (prune-var-ast v)])))))

(defn api-build
  ([expressions]
   (api-build expressions nil nil))
  ([expressions cursor]
   (api-build expressions cursor nil))
  ([expressions cursor connection-id]
   (api-build expressions cursor connection-id []))
  ([expressions cursor connection-id access-policy]
   (let [conn-id (or connection-id @db/connection-id)
         connection-name (connections/get-connection-name conn-id)]
     (try
       (let [exprs         (if (string? expressions) [expressions] expressions)
             context-exprs (butlast exprs)
             ;; nil (missing/absent last expression) is the only value the
             ;; parser can't handle - "" and blank strings parse fine into an
             ;; empty :table op, which is what lets an empty input still show
             ;; table hints on Tab instead of "nothing found".
             last-expr     (or (last exprs) "")]
         (let [{:keys [variables error]} (evaluate-expressions context-exprs conn-id access-policy)]
           (if error
             {:connection-id connection-name :error error}
             (let [result                    (generate-state last-expr cursor conn-id variables access-policy)
                   {state :result build-error :error} result]
               (if build-error
                 {:connection-id connection-name :error build-error}
                 {:connection-id connection-name
                  :version version
                  :query (-> last-expr trim-pipes (generate-state nil conn-id variables access-policy) :result eval/build-query eval/formatted-query)
                  :ast (prune-ast state)})))))
       (catch Exception e {:connection-id connection-name
                           :error (.getMessage e)})))))

(defn- get-columns
  ([rows]
   (if (seq rows)
     (mapv (fn [col] {:column col}) (first rows))
     []))
  ([state rows]
   (let [state-columns (-> state :columns)
         row-columns (if (seq rows)
                       (-> rows first)
                       [])
         remaining-columns (->> row-columns
                                (drop (count state-columns))
                                (map (fn [col] {:column col :alias (-> state :current)})))]
     (concat state-columns
             remaining-columns
             (when-let [alias (state :alias)]
               [alias])))))

(defn api-eval
  ([expressions]
   (api-eval expressions nil))
  ([expressions connection-id]
   (api-eval expressions connection-id []))
  ([expressions connection-id access-policy]
   (let [conn-id (or connection-id @db/connection-id)
         connection-name (connections/get-connection-name conn-id)]
     (try
       (let [exprs         (if (string? expressions) [expressions] expressions)
             context-exprs (butlast exprs)
             last-expr     (last exprs)
             trimmed       (trim-pipes (or last-expr ""))]
         (if (str/blank? trimmed)
           {:connection-id connection-name}
           (let [{:keys [variables error]} (evaluate-expressions context-exprs conn-id access-policy)]
             (if error
               {:connection-id connection-name :error error}
               (let [{last-state :result build-error :error} (generate-state trimmed nil conn-id variables access-policy)]
                 (if build-error
                   {:connection-id connection-name :error build-error}
                   (let [query (-> last-state eval/build-query eval/formatted-query)]
                     (try
                       (let [rows    (eval/run-query last-state)
                             op-type (get-in last-state [:operation :type])
                             columns (if (contains? #{:update-action :delete-action} op-type)
                                       (get-columns rows)
                                       (get-columns last-state rows))]
                         {:connection-id connection-name
                          :version version
                          :result rows
                          :columns columns})
                       (catch Exception e {:connection-id connection-name
                                           :error (.getMessage e)
                                           :query query})))))))))
       (catch Exception e {:connection-id connection-name
                           :error (.getMessage e)})))))

(defn get-connection []
  (let [connection-id   @db/connection-id]
    (if connection-id
      (let [connection-name (connections/get-connection-name connection-id)
            _               (db/init-references @db/connection-id)]
        {:result
         {:connection-id connection-name
          :version version}})
      {:result
       {:connection-id ""
        :version version}})))

(defn get-connections []
  {:result
   {:version version
    :selected-connection-id @db/connection-id
    :connections (connections/list-connections)}})

(defn test-connection [id]
  (let [result (db/run-query id {:query "SELECT NOW();"})]
    {:connection-id id :time result}))

(defn set-connection-pool [id]
  {:version version
   :connection-id (db/set-connection id)})

(defn create-connection [connection]
  (try
    {:connection-id (connections/add-connection-pool connection)}
    (catch Exception e {:error (.getMessage e)})))

(defn connect [id]
  (try
    (-> id test-connection :connection-id set-connection-pool)
    (catch Exception e {:error (.getMessage e)})))

(defn disconnect [id]
  (try
    (connections/remove-connection-pool id)
    (db/clear-connection-if id)
    (get-connections)
    (catch Exception e {:error (.getMessage e)})))

(defn reindex-connection [id]
  (try
    {:connection-id (db/reindex-references id)}
    (catch Exception e {:error (.getMessage e)})))

(defn api-sql
  ([sql-query]
   (api-sql sql-query nil))
  ([sql-query connection-id]
   (let [conn-id (or connection-id @db/connection-id)
         connection-name (connections/get-connection-name conn-id)]
     (cond
       (nil? sql-query)
       {:connection-id connection-name
        :error "SQL query is required. Please provide a 'query' parameter in the request body."}

       (clojure.string/blank? sql-query)
       {:connection-id connection-name
        :error "SQL query cannot be empty."}

       :else
       (try
         (let [result (db/run-sql conn-id sql-query)
               columns (when (vector? result) (get-columns result))]
           {:connection-id connection-name
            :version version
            :result result
            :columns columns})
         (catch Exception e {:connection-id connection-name
                             :error (.getMessage e)}))))))

(defn wrap-logger
  [handler]
  (fn [request]
    (let [response (handler request)]
      (when (= 404 (:status response))
        (prn (format "Path not found: %s" (:uri request))))
      response)))

;; TODO: POST method should return 401

(defroutes app-routes
  ;; connection management
  (GET "/api/v1/connection" [] (-> (get-connection) response))
  (GET "/api/v1/connections" [] (-> (get-connections) response))
  (POST "/api/v1/connections" req
    (let [connection (get-in req [:params])]
      (-> connection create-connection response)))
  (POST "/api/v1/connections/:id/connect" [id]
    (-> id connect response))
  (POST "/api/v1/connections/:id/reindex" [id]
    (-> id reindex-connection response))
  (DELETE "/api/v1/connections/:id" [id]
    (-> id disconnect response))
  (GET "/api/v1/connection/stats" []
    (-> {:connection-count (db/get-connection-count @db/connection-id)
         :version version
         :time (str (java.time.LocalDateTime/now))} response))

  ;; query building and evaluation
  (POST "/api/v1/build" {params :params}
    (let [{:keys [expressions expression cursor connection-id]} params
          exprs (or expressions (when expression [expression]))
          rules (access-policy/sanitize-rules (:access-policy params))]
      (->> (api-build exprs cursor connection-id rules) response)))
  (POST "/api/v1/eval" {params :params}
    (let [{:keys [expressions expression connection-id]} params
          exprs (or expressions (when expression [expression]))
          rules (access-policy/sanitize-rules (:access-policy params))]
      (->> (api-eval exprs connection-id rules) response)))

  ;; raw SQL execution
  (POST "/api/v1/sql" {params :params}
    (let [{:keys [query connection-id]} params]
      (->> (api-sql query connection-id) response)))

  ;; Legacy
  ;;
  ;; pine-mode.el
  (POST "/api/v1/build-with-params" {params :params}
    (let [{:keys [expression connection-id]} params]
      (->> (api-build [(trim-pipes expression)] nil connection-id) :query response)))
  ;; default case
  (route/not-found "Not Found"))
(def app
  (-> app-routes
      (wrap-json-params {:keywords? true})
      wrap-json-response
      wrap-logger
      (wrap-defaults api-defaults)
      (wrap-cors :access-control-allow-origin [#".*"]
                 :access-control-allow-methods [:get :post :put :delete])))
