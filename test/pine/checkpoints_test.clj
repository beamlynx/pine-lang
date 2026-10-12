(ns pine.checkpoints-test
  "The list of checkpoints /build reports in `ast.checkpoints`. See
  docs/checkpoints.md."
  (:require [clojure.string :as s]
            [clojure.test :refer [deftest is testing]]
            [pine.api :as api]))

(defn- build [expression]
  (api/api-build [expression] nil :test))

(defn- checkpoints [expression]
  (get-in (build expression) [:ast :checkpoints]))

(defn- op-text
  "The text of operation `index`, as the client slices it out of
  `ast.ranges`. Single-line expressions only."
  [expression index]
  (let [{:keys [start end]} (nth (get-in (build expression) [:ast :ranges]) index)]
    (s/trim (subs expression (:character start) (:character end)))))

(deftest test-plain-limit
  (testing "A trailing limit is a pending checkpoint, under the name sealing it would give"
    (is (= [{:name "result_1" :sealed false :index 1 :kind "limit" :limit 10
             :tables ["c_0"] :columns ["id" "created_at"]}]
           (checkpoints "company | l: 10"))))

  (testing "Once something follows it, it is sealed"
    (is (= [{:name "result_1" :sealed true :index 1 :kind "limit" :limit 10
             :tables ["c_0"] :columns ["id" "created_at"]}]
           (checkpoints "company | l: 10 | employee")))))

(deftest test-limit-per
  (testing "per columns are resolved to the alias they belong to"
    (is (= [{:name "result_1" :sealed true :index 3 :kind "limit" :limit 1
             :per [{:alias "c" :column "id"}]
             :tables ["c" "e_1"]
             ;; Only the current table's columns: what the CTE selects.
             :columns ["id" "company_id" "reports_to"]}]
           (checkpoints "company as c | employee .company_id | o: created_at desc | l: 1 per c.id | where: id = 1"))))

  (testing "A key inside a JSON column keeps its path"
    (is (= [{:alias "c_0" :column "data" :path ["region"] :column-alias "data.region"}]
           (:per (first (checkpoints "customer | l: 1 per data.region")))))))

(deftest test-group
  (testing "A group checkpoint has its columns and aggregates, and returns only those"
    (is (= [{:name "result_1" :sealed true :index 1 :kind "group"
             :group [{:alias "c_0" :column "name"}]
             :aggregates ["count"]
             :tables ["c_0"]
             :columns ["name" "count"]}]
           (checkpoints "company | group: name | where: count > 1")))))

(deftest test-user-named
  (testing "A `|= name` names the checkpoint, sealed or not"
    (is (= ["pg" true] ((juxt :name :sealed) (first (checkpoints "company | l: 10 |= pg | employee")))))
    (is (= ["pg" false] ((juxt :name :sealed) (first (checkpoints "company | l: 10 |= pg")))))))

(deftest test-stacked
  (testing "Two checkpoints, in pipe order; the second seals the first's name"
    (let [[a b] (checkpoints "company | l: 10 | employee | l: 5")]
      (is (= ["result_1" true 1 ["c_0"]] ((juxt :name :sealed :index :tables) a)))
      (is (= ["result_2" false 3 ["result_1" "e_1"] ["id" "company_id" "reports_to"]]
             ((juxt :name :sealed :index :tables :columns) b)))))

  (testing "A limit after a group returns what the group returned"
    (let [[a b] (checkpoints "company | group: name | l: 3")]
      (is (= ["group" ["name" "count"]] ((juxt :kind :columns) a)))
      (is (= ["limit" 3 ["result_1"] ["name" "count"]] ((juxt :kind :limit :tables :columns) b))))))

(deftest test-held-by-count
  (testing "count: doesn't seal a checkpoint, so it is still reported as pending"
    (is (= [["result_1" false]]
           (map (juxt :name :sealed) (checkpoints "company | l: 10 | count:"))))))

(deftest test-partial
  (testing "A limit: N per still being typed is already a checkpoint"
    (is (= [["limit" 1 false]]
           (map (juxt :kind :limit :sealed) (checkpoints "company | l: 1 per "))))))

(deftest test-no-checkpoint
  (is (= [] (checkpoints "company | where: id = 1"))))

(deftest test-index-matches-ranges
  (testing ":index points at the group:/limit: step in ast.ranges"
    (doseq [e ["company | l: 10"
               "company as c | employee .company_id | l: 1 per c.id |= x | document"
               "company | where: id = 1 | group: name | l: 3"
               "company | l: 10 | employee | o: id | l: 5 | s: id"]]
      (doseq [{:keys [index]} (checkpoints e)]
        (is (re-find #"^(l:|limit:|g:|group:)" (op-text e index))
            (str e " @ " index))))))

(deftest test-sealed-stage-in-pending-assignments
  (testing "A sealed stage's where/order/group reach the client with its snapshot"
    (let [ast (:ast (build "company as c | where: id = 1 | o: id desc | l: 10 | employee"))]
      (is (= ["c"] (map :alias (get-in ast [:pending-assignments "result_1" :where]))))
      (is (= ["c"] (map :alias (get-in ast [:pending-assignments "result_1" :order])))))))
