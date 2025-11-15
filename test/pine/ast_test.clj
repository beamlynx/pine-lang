(ns pine.ast-test
  (:require [clojure.test :refer [deftest is testing]]
            [pine.parser :as parser]
            [pine.ast.main :as ast]
            [pine.data-types :as dt]))

(defn- generate
  "Helper function to generate and get the relevant part in the ast"
  ([expression]
   (generate identity expression))
  ([type expression]
   (let [ast (-> expression
                 parser/parse
                 :result
                 (ast/generate :test))]
     (if (sequential? type)
       (mapv #(get ast %) type)
       (get ast type)))))

(deftest test-ast

  (testing "Generate ast for `tables`"
    (is (= [{:schema nil :table "company" :alias "c" :parent nil :join-column nil :join-left-column nil :join-right-column nil :join nil}]
           (generate :tables "company as c")))
    (is (= [{:schema nil :table "user" :alias "u_0" :parent nil  :join-column nil :join-left-column nil :join-right-column nil :join nil}]
           (generate :tables "user")))
    (is (= [{:schema "public" :table "user" :alias "u_0" :parent nil  :join-column nil :join-left-column nil :join-right-column nil :join nil}]
           (generate :tables "public.user")))
    (is (= [{:schema "public" :table "user" :alias "u_0" :parent true  :join-column nil :join-left-column nil :join-right-column nil :join nil}]
           (generate :tables "public.user :parent"))))

  (testing "Generate ast for `tables` with join types"
    (is (= [{:schema nil :table "user" :alias "u_0" :parent nil :join-column nil :join-left-column nil :join-right-column nil :join "LEFT"}]
           (generate :tables "user :left")))
    (is (= [{:schema nil :table "user" :alias "u_0" :parent nil :join-column nil :join-left-column nil :join-right-column nil :join "RIGHT"}]
           (generate :tables "user :right")))
    (is (= [{:schema "public" :table "user" :alias "u_0" :parent nil :join-column nil :join-left-column nil :join-right-column nil :join "LEFT"}]
           (generate :tables "public.user :left")))
    (is (= [{:schema "public" :table "user" :alias "u_0" :parent nil :join-column nil :join-left-column nil :join-right-column nil :join "RIGHT"}]
           (generate :tables "public.user :right"))))

  (testing "Generate ast for `from`"
    (is (= "c"
           (generate :context "company as c | user | from: c | ")))
    (is (= "c"
           (generate :context "company as c | user | from: c | 1"))))

  (testing "Generate ast for `select`"
    (is (= [{:alias "c_0" :column "id" :operation-index 1} {:alias "c_0" :column "id" :column-alias "__c_0__id" :operation-index 2 :auto-id true :hidden true}]
           (generate :columns "company | s: id")))
    (is (= [{:alias "c_0" :column "id" :column-alias "c_id" :operation-index 1} {:alias "c_0" :column "id" :column-alias "__c_0__id" :operation-index 2 :auto-id true :hidden true}]
           (generate :columns "company | s: id as c_id")))
    (is (= [{:alias "c_0" :column "id" :operation-index 1} {:alias "e_1" :column "id" :operation-index 3}
            {:alias "c_0" :column "id" :column-alias "__c_0__id" :operation-index 4 :auto-id true :hidden true}
            {:alias "e_1" :column "id" :column-alias "__e_1__id" :operation-index 5 :auto-id true :hidden true}]
           (generate :columns "company | s: id | employee | s: id")))
    (is (= [{:alias "c" :column "id" :operation-index 1} {:alias "c" :column "id" :column-alias "__c__id" :operation-index 2 :auto-id true :hidden true}]
           (generate :columns "company as c | s: id")))
    (is (= [{:alias "u_0" :column "id" :column-alias "__u_0__id" :operation-index 1 :auto-id true :hidden true}]
           (generate :columns "user")))
    (is (= [{:alias "u" :column "id" :operation-index 1} {:alias "u" :column "id" :column-alias "__u__id" :operation-index 3 :auto-id true :hidden true}]
           (generate :columns "user as u | s: id | limit: 1")))
    (is (= [{:alias "c" :column "id" :operation-index 1}
            {:alias "u" :column "id" :operation-index 3}
            {:alias "c" :column "id" :column-alias "__c__id" :operation-index 5 :auto-id true :hidden true}
            {:alias "u" :column "id" :column-alias "__u__id" :operation-index 6 :auto-id true :hidden true}]
           (generate :columns "customer as c | s: id | user as u | s: id | limit: 1")))
    (is (= [{:alias "u" :column "" :symbol "*" :operation-index 1} {:alias "u" :column "id" :column-alias "__u__id" :operation-index 2 :auto-id true :hidden true}]
           (generate :columns "user as u | s: u.*"))))

  (testing "Generate ast for `order`"
    (is (= [{:alias "c_0" :column "country" :direction "DESC" :operation-index 1}]
           (generate :order "company | o: country")))
    (is (= [{:alias "c_0" :column "country" :direction "DESC" :operation-index 1}
            {:alias "c_0" :column "created_at" :direction "DESC" :operation-index 1}]
           (generate :order "company | o: country, created_at")))
    ;; Test aliased order columns
    (is (= [{:alias "e" :column "name" :direction "DESC" :operation-index 1}]
           (generate :order "employee as e | o: e.name")))
    (is (= [{:alias "e" :column "name" :direction "ASC" :operation-index 1}]
           (generate :order "employee as e | o: e.name asc")))
    (is (= [{:alias "e" :column "name" :direction "DESC" :operation-index 1}
            {:alias "e" :column "created_at" :direction "ASC" :operation-index 1}]
           (generate :order "employee as e | o: e.name, e.created_at asc")))

    ;; Test mixed aliased and non-aliased order columns
    (is (= [{:alias "c_0" :column "name" :direction "DESC" :operation-index 1}
            {:alias "c_0" :column "age" :direction "DESC" :operation-index 1}]
           (generate :order "company | o: name desc, age desc")))
    (is (= [{:alias "e" :column "name" :direction "DESC" :operation-index 2}
            {:alias "d_1" :column "title" :direction "ASC" :operation-index 2}]
           (generate :order "employee as e | document | o: e.name desc, title asc"))))

  (testing "Generate ast for `limit`"
    (is (= 10
           (generate :limit "limit: 10")))
    (is (= 1
           (generate :limit "l: 1"))))

  (testing "Generate ast for `where`"
    (is (= [[nil "name" nil "=" (dt/string "Acme")]]
           (generate :where "name = 'Acme'")))
    (is (= [[nil "id" nil "=" (dt/number "1")]]
           (generate :where "id = 1")))
    (is (= [["c_0" "name" nil "=" (dt/string "Acme")]]
           (generate :where "company | name = 'Acme'")))
    (is (= [["c_0" "name" "text" "=" (dt/string "Acme")]]
           (generate :where "company | name = 'Acme' ::text")))
    (is (= [["c_0" "id" "uuid" "=" (dt/string "123e4567-e89b-12d3-a456-426614174000")]]
           (generate :where "company | id = '123e4567-e89b-12d3-a456-426614174000' ::uuid")))
    (is (= [["c" "name" nil "=" (dt/string "Acme")]]
           (generate :where "company as c | name = 'Acme'")))
    (is (= [["c" "name" nil "=" (dt/string "Acme")] ["c" "country" nil "=" (dt/string "PK")]]
           (generate :where "company as c | name = 'Acme' | country = 'PK'")))
    (is (= [["c" "country" nil "IN" [(dt/string "PK") (dt/string "DK")]]]
           (generate :where "company as c | country in ('PK', 'DK')")))
    (is (= [[nil "name" nil "LIKE" (dt/string "Acme%")]]
           (generate :where "name like 'Acme%'")))
    (is (= [[nil "name" nil "NOT LIKE" (dt/string "Acme%")]]
           (generate :where "name not like 'Acme%'")))
    (is (= [[nil "name" nil "ILIKE" (dt/string "acme%")]]
           (generate :where "name ilike 'acme%'")))
    (is (= [[nil "name" nil "NOT ILIKE" (dt/string "acme%")]]
           (generate :where "name not ilike 'acme%'"))))

  (testing "Generate ast for `where` with dates"
    (is (= [[nil "created_at" nil "=" (dt/date "2025-01-01")]]
           (generate :where "created_at = '2025-01-01'")))
    (is (= [[nil "created_at" nil "!=" (dt/date "2025-01-01")]]
           (generate :where "created_at != '2025-01-01'")))
    (is (= [[nil "created_at" nil ">" (dt/date "2025-01-01")]]
           (generate :where "created_at > '2025-01-01'")))
    (is (= [[nil "created_at" nil "<" (dt/date "2025-01-01")]]
           (generate :where "created_at < '2025-01-01'"))))

  (testing "Generate ast for `join` where there is no relation"
    (is (= [["a_0" "b_1" nil nil]]
           (generate :joins "a | b")))
    (is (= [["a_0" "b_1" nil nil]]
           (generate :joins "a | b .a_id")))

    ;; Explicit join columns
    (is (= [["a_0" "b_1" ["a_0" "id" :has "b_1" "a_id"] nil]]
           (generate :joins "a | b .a_id = .id"))))

  (testing "Generate ast for `join` where there is a relation"
    (is (= [["c_0" "e_1" ["c_0" "id" :has "e_1" "company_id"] nil]]
           (generate :joins "company | employee")))
    (is (= [["c_0" "e_1" ["c_0" "id" :has "e_1" "company_id"] nil]]
           (generate :joins "company | employee .company_id")))
    (is (= [["c_0" "e_1" ["c_0" nil :has "e_1" nil] nil]]
           (generate :joins "company | employee .employee_id"))) ;; trying with incorrect id
    )
  (testing "Generate ast for `join` where there is ambiguity"
    (is (= [["e_0" "d_1" ["e_0" "id" :has "d_1" "created_by"] nil]]
           (generate :joins "employee | document .created_by")))
    (is (= [["e_0" "d_1" ["e_0" "id" :has "d_1" "employee_id"] nil]]
           (generate :joins "employee | document .employee_id"))))

  (testing "Generate ast for `join` using self join"
    ;; By default, we narrow the results
    ;; i.e. we join with the child
    (is (= [["e_0" "e_1" ["e_0" "id" :has "e_1" "reports_to"] nil]]
           (generate :joins "employee | employee")))
    (is (= [["e_0" "e_1" ["e_0" "id" :has "e_1" "reports_to"] nil]]
           (generate :joins "employee | employee .reports_to")))

    ;; However, we can exlicitly saw that the table is a parent using the `^` character
    (is (= [["e_0" "e_1" ["e_0" "reports_to" :of "e_1" "id"] nil]]
           (generate :joins "employee | employee :parent")))
    (is (= [["e_0" "e_1" ["e_0" "reports_to" :of "e_1" "id"] nil]]
           (generate :joins "employee | employee :parent .reports_to"))))

  (testing "Generate ast for `join` with explicit columns"
    ;; Basic explicit columns with real tables
    (is (= [["c_0" "e_1" ["c_0" "id" :has "e_1" "company_id"] nil]]
           (generate :joins "company | employee .company_id = .id")))

    ;; Explicit columns with different column names
    (is (= [["a_0" "b_1" ["a_0" "custom_id" :has "b_1" "foreign_id"] nil]]
           (generate :joins "a | b .foreign_id = .custom_id")))

    ;; Explicit columns with join type
    (is (= [["c_0" "e_1" ["c_0" "id" :has "e_1" "company_id"] "LEFT"]]
           (generate :joins "company | employee .company_id = .id :left")))

    (is (= [["c_0" "e_1" ["c_0" "id" :has "e_1" "company_id"] "RIGHT"]]
           (generate :joins "company | employee .company_id = .id :right"))))

  (testing "Generate ast for `count`"
    (is (= {:column "*"} (generate :count "company | count:"))))

  (testing "Generate ast for `delete`"
    (is (= {:column "id"} (generate :delete "company | delete! .id"))))

  (testing "Generate ast for `update`"
    (is (= {:assignments [{:column {:alias nil :column "name"} :value (dt/string "John Doe")}]}
           (generate :update "company | update! name = 'John Doe'")))
    (is (= {:assignments [{:column {:alias nil :column "name"} :value (dt/string "John")}
                          {:column {:alias nil :column "age"} :value (dt/number "30")}]}
           (generate :update "company | update! name = 'John', age = 30"))))

  (testing "Generate ast for `group`"
    (is (= [[{:alias "c" :column "status" :operation-index 1} {:symbol "COUNT(1)" :column-alias "count"}]
            [{:alias "c" :column "status" :operation-index 1}]]
           (generate [:columns :group] "company as c | group: c.status => count"))))

  (testing "Generate ast for date extraction functions"
    ;; Helper to filter out auto-id columns
    (let [non-auto #(filter (fn [c] (not (:auto-id c))) %)]
      ;; Year extraction creates 1 column with col-fn
      (is (= [{:column "created_at" :alias "e_0" :column-alias "year"
               :col-fn "year" :operation-index 1}]
             (non-auto (generate :columns "employee | select: created_at => year"))))

      ;; Month extraction creates 1 column with col-fn
      (is (= [{:column "created_at" :alias "e_0" :column-alias "month"
               :col-fn "month" :operation-index 1}]
             (non-auto (generate :columns "employee | select: created_at => month"))))

      ;; Day extraction creates 1 column with col-fn
      (is (= [{:column "created_at" :alias "e_0" :column-alias "day"
               :col-fn "day" :operation-index 1}]
             (non-auto (generate :columns "employee | select: created_at => day"))))

      ;; Week extraction creates 1 column with col-fn
      (is (= [{:column "created_at" :alias "e_0" :column-alias "week"
               :col-fn "week" :operation-index 1}]
             (non-auto (generate :columns "employee | select: created_at => week"))))

      ;; Hour extraction creates 1 column with col-fn
      (is (= [{:column "created_at" :alias "e_0" :column-alias "hour"
               :col-fn "hour" :operation-index 1}]
             (non-auto (generate :columns "employee | select: created_at => hour"))))

      ;; Minute extraction creates 1 column with col-fn
      (is (= [{:column "created_at" :alias "e_0" :column-alias "minute"
               :col-fn "minute" :operation-index 1}]
             (non-auto (generate :columns "employee | select: created_at => minute"))))

      ;; With alias
      (is (= [{:column "created_at" :alias "e" :column-alias "month"
               :col-fn "month" :operation-index 1}]
             (non-auto (generate :columns "employee as e | select: e.created_at => month"))))

      ;; Mixed with regular columns (excluding auto-id)
      (let [cols (non-auto (generate :columns "employee | select: name, created_at => year"))]
        (is (= 2 (count cols)))
        (is (= "name" (:column (first cols))))
        (is (= "created_at" (:column (second cols))))
        (is (= "year" (:col-fn (second cols)))))))

  (testing "Generate ast for `where-partial`"
    ;; Empty where-partial should have no conditions in :where
    (is (= []
           (generate :where "company | w:")))

    ;; Just partial column should have no conditions in :where  
    (is (= []
           (generate :where "company | w: i")))

    ;; Column + operator should have no conditions in :where
    (is (= []
           (generate :where "company | w: id =")))

    ;; Verify the operation structure for different where-partial cases
    (is (= {:type :where-partial
            :value {:complete-conditions [] :partial-condition nil}}
           (generate :operation "company | w:")))

    (is (= {:type :where-partial
            :value {:complete-conditions [] :partial-condition {:column "id"}}}
           (generate :operation "company | w: id")))

    (is (= {:type :where-partial
            :value {:complete-conditions [] :partial-condition {:alias "c" :column "name"}}}
           (generate :operation "company as c | w: c.name")))

    (is (= {:type :where-partial
            :value {:complete-conditions [] :partial-condition {:column "id" :operator :equals}}}
           (generate :operation "company | w: id =")))

    (is (= {:type :where-partial
            :value {:complete-conditions [] :partial-condition {:column "name" :operator :like}}}
           (generate :operation "company | w: name like"))))

  (testing "Schema-based type conversion in UPDATE operations"
    ;; Test that JSONB column gets proper type conversion
    (is (= {:assignments [{:column {:alias nil :column "data"}
                           :value (dt/jsonb "{\"test\": 1}")}]}
           (generate :update "customer | update! data = '{\"test\": 1}'")))

    ;; Test that string column remains string - need to add name column to customer in fixtures
    ;; (is (= {:assignments [{:column {:alias nil :column "name"} 
    ;;                       :value (dt/string "John")}]}
    ;;        (generate :update "customer | update! name = 'John'")))
    )

  (testing "Schema-based type conversion in WHERE operations"
    ;; Test that JSONB column gets proper type conversion in WHERE clause
    (let [where-result (generate :where "customer | w: data = '{\"key\": \"value\"}'")]
      (is (= 1 (count where-result)))
      (let [[alias col _ operator value] (first where-result)]
        (is (= alias "c_0"))
        (is (= col "data"))
        (is (= operator "="))
        (is (= (:type value) :jsonb))
        (is (= (:value value) "{\"key\": \"value\"}"))))))

(testing "AST generation with comments"
  (is (= [{:schema nil :table "company" :alias "c_0" :parent nil :join-column nil :join-left-column nil :join-right-column nil :join nil}]
         (generate :tables "-- get all companies\ncompany")))
  (is (= [[nil "name" nil "=" (dt/string "Acme")]]
         (generate :where "/* filter by name */ name = 'Acme'")))
  (is (= 10
         (generate :limit "limit: 10 -- max results"))))