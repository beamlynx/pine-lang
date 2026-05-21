(ns pine.api
  (:require
   [cheshire.generate :refer [add-encoder encode-str]]
   [clojure.string :as str]
   [compojure.core :refer [defroutes GET POST]]
   [compojure.route :as route]
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
   (generate-state expression nil nil))
  ([expression cursor]
   (generate-state expression cursor nil))
  ([expression cursor connection-id]
   (let [{:keys [result error]} (->> expression parser/parse)
         conn-id (or connection-id @db/connection-id)]
     (if result {:result (ast/generate result conn-id expression cursor)}
         {:error-type "parse"
          :error error}))))

(defn- trim-pipes [s]
  (-> s
      (str/trim)
      (str/replace #"^\|\s*|\s*\|$" "")
      (str/trim)))

(defn api-build
  ([expression]
   (api-build expression nil nil))
  ([expression cursor]
   (api-build expression cursor nil))
  ([expression cursor connection-id]
   (let [conn-id (or connection-id @db/connection-id)
         connection-name (connections/get-connection-name conn-id)]
     (try
       (let [result (generate-state expression cursor conn-id)
             {state :result error :error} result]
         (if error result
             {:connection-id connection-name
              :version version
              :query (-> expression trim-pipes (generate-state nil conn-id) :result eval/build-query eval/formatted-query)
              :ast (select-keys state [:hints :selected-tables :joins :context :current :operation :columns :order :where :prettified :ranges])}))
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
  ([expression]
   (api-eval expression nil))
  ([expression connection-id]
   (let [conn-id (or connection-id @db/connection-id)
         connection-name (connections/get-connection-name conn-id)]
     (try
       (let [result (generate-state expression nil conn-id)
             {state :result error :error} result]
         (if error result
             (let [rows (eval/run-query state)
                   op-type (get-in state [:operation :type])
                   ;; For action results we control the format; columns come from header row
                   columns (if (contains? #{:update-action :delete-action} op-type)
                             (get-columns rows)
                             (get-columns state rows))]
               {:connection-id connection-name
                :version version
                 ;;  :time (db/run-query (state :connection-id) {:query "SELECT NOW() as now, NOW() AT TIME ZONE 'UTC' AS utc;"})
                 ;;  :server_time (str (java.time.Instant/now))
                :result rows
                :columns columns})))

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

(defn connect [id]
  (try
    (-> id test-connection :connection-id set-connection-pool)
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
      (-> {:connection-id (connections/add-connection-pool connection)} response)))
  (POST "/api/v1/connections/:id/connect" [id]
    (-> id connect response))
  (GET "/api/v1/connection/stats" []
    (-> {:connection-count (db/get-connection-count @db/connection-id)
         :version version
         :time (str (java.time.LocalDateTime/now))} response))

  ;; query building and evaluation
  (POST "/api/v1/build" {params :params}
    (let [{:keys [expression cursor connection-id]} params]
      (->> (api-build expression cursor connection-id) response)))
  (POST "/api/v1/eval" {params :params}
    (let [{:keys [expression connection-id]} params]
      (->> (api-eval (trim-pipes expression) connection-id) response)))

  ;; raw SQL execution
  (POST "/api/v1/sql" {params :params}
    (let [{:keys [query connection-id]} params]
      (->> (api-sql query connection-id) response)))

  ;; Legacy
  ;;
  ;; pine-mode.el
  (POST "/api/v1/build-with-params" {params :params}
    (let [{:keys [expression connection-id]} params]
      (->> (api-build (trim-pipes expression) nil connection-id) :query response)))
  ;; default case
  (route/not-found "Not Found"))
(def app
  (-> app-routes
      (wrap-json-params {:keywords? true})
      wrap-json-response
      wrap-logger
      (wrap-defaults api-defaults)
      (wrap-cors :access-control-allow-origin [#".*"]
                 :access-control-allow-methods [:get :post :put])))
