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

(defn- generate-state [expression]
  (let [{:keys [result error]} (->> expression parser/parse)]
    (if result {:result (-> result ast/generate)}
        {:error-type "parse"
         :error error})))

(defn- trim-pipes [s]
  (-> s
      (str/trim)
      (str/replace #"^\|\s*|\s*\|$" "")
      (str/trim)))

(defn api-build [expression]
  (let [connection-name (connections/get-connection-name @db/connection-id)]
    (try
      (let [result (generate-state expression)
            {state :result error :error} result]
        (if error result
            {:connection-id connection-name
             :version version
             :query (-> expression trim-pipes generate-state :result eval/build-query eval/formatted-query)
             :ast (dissoc state :references :join-map)}))
      (catch Exception e {:connection-id connection-name
                          :error (.getMessage e)}))))

(defn- get-columns [state rows]
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
              [alias]))))

(defn api-eval [expression]
  (let [connection-name (connections/get-connection-name @db/connection-id)]
    (try
      (let [result (generate-state expression)
            {state :result error :error} result]
        (if error result
            (let [rows (eval/run-query state)]
              {:connection-id connection-name
               :version version
                ;;  :time (db/run-query (state :connection-id) {:query "SELECT NOW() as now, NOW() AT TIME ZONE 'UTC' AS utc;"})
                ;;  :server_time (str (java.time.Instant/now))
               :result rows
               :columns (get-columns state rows)})))

      (catch Exception e {:connection-id connection-name
                          :error (.getMessage e)}))))

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

(defn wrap-logger
  [handler]
  (fn [request]
    (let [response (handler request)]
      (when (= 404 (:status response))
        (prn (format "Path not found: %s" (:uri request))))
      response)))

;; TODO: POST method should return 401

(defroutes app-routes
  (POST "/api/v1/build" [expression] (->> expression api-build response))
  (GET "/api/v1/connection" [] (-> (get-connection) response))
  (GET "/api/v1/connections" [] (-> @connections/pools response))
  (POST "/api/v1/connections" req
    (let [connection (get-in req [:params])]
      (-> {:connection-id (connections/add-connection-pool connection)} response)))

  (POST "/api/v1/connections/:id/connect" [id]
    (-> id connect response))

  (GET "/api/v1/connection/stats" []
    (-> {:connection-count (db/get-connection-count @db/connection-id)
         :version version
         :time (str (java.time.LocalDateTime/now))} response))

  (POST "/api/v1/eval" [expression] (->> expression trim-pipes api-eval response))
  ;; pine-mode.el
  (POST "/api/v1/build-with-params" [expression] (->> expression trim-pipes api-build :query response))
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
