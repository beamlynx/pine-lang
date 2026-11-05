(ns pine.path-search-test
  "How much work a build does for `? target` on a large schema. The path
  search can use its whole time budget, so a build runs it at most once,
  and a repeat of the same question doesn't run it at all."
  (:require [clojure.test :refer [deftest is testing]]
            [pine.api :as api]
            [pine.ast.hints :as hints]
            [pine.ast.main :as ast]
            [pine.eval :as eval]
            [pine.parser :as parser]
            [pine.db.main :as db]
            [pine.db.references :as refs]))

(defn- hub-schema
  "403 tables: three hubs, and 400 tables that each point at all three.
  Every table reaches every other in two hops, through any hub, which is
  what makes a search here expensive."
  []
  (let [hubs    (map #(str "hub_" %) (range 3))
        tables  (map #(str "tbl_" %) (range 400))
        columns (concat (for [h hubs] ["public" h "id" 1 "integer" nil "NO" nil])
                        (for [t tables c (cons "id" (map #(str % "_id") hubs))]
                          ["public" t c 1 "integer" nil "YES" nil]))
        fks     (for [t tables h hubs] ["public" t (str h "_id") "public" h "id" (str t "_" h "_fkey") 1])
        pks     (for [t (concat hubs tables)] ["public" t "id" 1])]
    (refs/index-references [fks columns pks])))

(defn- counting
  "Wraps the function in var v to count its calls in counter."
  [counter v]
  (let [f @v]
    (fn [& args] (swap! counter update v (fnil inc 0)) (apply f args))))

(defn- timed [f]
  (let [start (System/nanoTime)
        result (f)]
    [(/ (- (System/nanoTime) start) 1e6) result]))

(def ^:private expression "tbl_0 | ? tbl_200")
(def ^:private cursor {:line 0 :character (count expression)})

(defn- build-at-end [expressions]
  (api/api-build expressions {:line 0 :character (count (last expressions))} :test))

(deftest test-path-search-runs-once-per-build
  (let [schema (hub-schema)
        calls  (atom {})]
    (with-redefs [db/init-references (fn [_] schema)
                  hints/handle (counting calls #'hints/handle)
                  hints/find-table-paths (counting calls #'hints/find-table-paths)]
      (testing "a build runs the hints, and so the search, once"
        (let [[ms response] (timed #(build-at-end [expression]))]
          (println (format "path search on a 403-table hub schema, first build: %.1f ms" ms))
          (is (nil? (:error response)))
          (is (= 1 (get @calls #'hints/handle)))
          (is (= 1 (get @calls #'hints/find-table-paths)))))

      ;; This schema is big enough that the deadline cuts every search
      ;; short, and a search cut short gets one more try.
      (testing "the same question again searches once more at most"
        (reset! calls {})
        (build-at-end [expression])
        (is (>= 1 (get @calls #'hints/find-table-paths 0))))

      (testing "and after that, not at all"
        (reset! calls {})
        (let [[ms _] (timed #(build-at-end [expression]))]
          (println (format "path search on a 403-table hub schema, repeat build: %.1f ms" ms))
          (is (= 1 (get @calls #'hints/handle)))
          (is (nil? (get @calls #'hints/find-table-paths)))
          ;; The search alone takes its whole 150 ms budget here.
          (is (< ms 100))))))

  (testing "blocks before the last one get no hints"
    (let [schema (hub-schema)
          calls  (atom {})]
      (with-redefs [db/init-references (fn [_] schema)
                    hints/handle (counting calls #'hints/handle)
                    hints/find-table-paths (counting calls #'hints/find-table-paths)]
        (build-at-end ["tbl_1 | ? tbl_300" expression])
        (is (= 1 (get @calls #'hints/handle)))
        (is (= 1 (get @calls #'hints/find-table-paths)))))))

(deftest test-path-cache-returns-the-same-paths
  (testing "a search that finishes is remembered as it was"
    ;; A new index of the test schema, so no earlier test's search is in
    ;; the cache.
    (let [calls  (atom {})
          schema (db/get-indexed-references :test)]
      (with-redefs [db/init-references (fn [_] schema)
                    hints/find-table-paths (counting calls #'hints/find-table-paths)]
        (let [first-paths  (get-in (build-at-end ["department | ? shift"]) [:ast :hints :paths])
              second-paths (get-in (build-at-end ["department | ? shift"]) [:ast :hints :paths])]
          (is (seq first-paths))
          (is (= first-paths second-paths))
          (is (= 1 (get @calls #'hints/find-table-paths))))))))

(deftest test-path-cache-follows-the-schema
  (let [calls (atom {})]
    (with-redefs [hints/find-table-paths (counting calls #'hints/find-table-paths)]
      (testing "a new schema index, as after a reindex, searches again"
        (doseq [schema [(hub-schema) (hub-schema)]]
          (with-redefs [db/init-references (fn [_] schema)]
            (api/api-build [expression] cursor :test)))
        (is (= 2 (get @calls #'hints/find-table-paths))))

      (testing "a different question searches again"
        (reset! calls {})
        (let [schema (hub-schema)]
          (with-redefs [db/init-references (fn [_] schema)]
            (api/api-build ["tbl_0 | ? tbl_200"] nil :test)
            (api/api-build ["tbl_0 | ? tbl_201"] nil :test)
            (api/api-build ["tbl_1 | ? tbl_200"] nil :test)
            (api/api-build ["tbl_0 | tbl_1 | ? tbl_200"] nil :test)))
        (is (= 4 (get @calls #'hints/find-table-paths)))))))

(defn- sql [generate expr]
  (-> expr parser/parse :result generate eval/build-query eval/formatted-query))

(deftest test-query-without-hints-is-the-same-sql
  (testing "the SQL doesn't depend on whether hints and prettify ran"
    (doseq [expr ["company | employee | where: id = 1 | s: id"
                  "company | employee"
                  "company | l: 1 | employee | s: id"
                  "company | group: id with count"
                  "document | o: id desc | l: 2 per company_id"
                  "employee | where: id = 1 | update! company_id = 1"
                  "company | count:"
                  "company | s: id |= c | employee"]]
      (is (= (sql #(ast/generate % :test expr nil {} []) expr)
             (sql #(ast/generate-for-query % :test expr {} []) expr))
          expr))))
