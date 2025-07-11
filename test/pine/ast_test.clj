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
    (is (= [{:schema nil :table "company" :alias "c" :parent nil :join-column nil :join nil}]
           (generate :tables "company as c")))
    (is (= [{:schema nil :table "user" :alias "u_0" :parent nil  :join-column nil :join nil}]
           (generate :tables "user")))
    (is (= [{:schema "public" :table "user" :alias "u_0" :parent nil  :join-column nil :join nil}]
           (generate :tables "public.user")))
    (is (= [{:schema "public" :table "user" :alias "u_0" :parent true  :join-column nil :join nil}]
           (generate :tables "public.user :parent"))))

  (testing "Generate ast for `tables` with join types"
    (is (= [{:schema nil :table "user" :alias "u_0" :parent nil :join-column nil :join "LEFT"}]
           (generate :tables "user :left")))
    (is (= [{:schema nil :table "user" :alias "u_0" :parent nil :join-column nil :join "RIGHT"}]
           (generate :tables "user :right")))
    (is (= [{:schema "public" :table "user" :alias "u_0" :parent nil :join-column nil :join "LEFT"}]
           (generate :tables "public.user :left")))
    (is (= [{:schema "public" :table "user" :alias "u_0" :parent nil :join-column nil :join "RIGHT"}]
           (generate :tables "public.user :right"))))

  (testing "Generate ast for `from`"
    (is (= "c"
           (generate :context "company as c | user | from: c | ")))
    (is (= "c"
           (generate :context "company as c | user | from: c | 1"))))

  (testing "Generate ast for `select`"
    (is (= [{:alias "c_0" :column "id" :index 1}]
           (generate :columns "company | s: id")))

    (is (= [{:alias "u" :column "id" :index 1}]
           (generate :columns "company | s: u.id")))
    (is (= [{:alias "c_0" :column "id" :column-alias "c_id" :index 1}]
           (generate :columns "company | s: id as c_id")))
    (is (= [{:alias "c_0" :column "id" :index 1} {:alias "e_1" :column "id" :index 3}]
           (generate :columns "company | s: id | employee | s: id")))
    (is (= [{:alias "c" :column "id" :index 1}]
           (generate :columns "company as c | s: id")))
    (is (= []
           (generate :columns "user")))
    (is (= [{:alias "u" :column "" :symbol "*" :index 1}]
           (generate :columns "user as u | s: u.*"))))

  (testing "Generate ast for `order`"
    (is (= [{:alias "c_0" :column "country" :direction "DESC" :index 1}]
           (generate :order "company | o: country")))
    (is (= [{:alias "c_0" :column "country" :direction "DESC" :index 1}
            {:alias "c_0" :column "created_at" :direction "DESC" :index 1}]
           (generate :order "company | o: country, created_at"))))

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
    (is (= [{"a_0" {"b_1" nil}} [["a_0" "b_1" nil nil]]]
           (generate [:join-map :joins] "a | b")))
    (is (= [{"a_0" {"b_1" nil}} [["a_0" "b_1" nil nil]]]
           (generate [:join-map :joins] "a | b .a_id"))))

  (testing "Generate ast for `join` where there is a relation"
    (is (= [{"c_0" {"e_1" ["c_0" "id" :has "e_1" "company_id"]}} [["c_0" "e_1" ["c_0" "id" :has "e_1" "company_id"] nil]]]
           (generate [:join-map :joins] "company | employee")))
    (is (= [{"c_0" {"e_1" ["c_0" "id" :has "e_1" "company_id"]}} [["c_0" "e_1" ["c_0" "id" :has "e_1" "company_id"] nil]]]
           (generate [:join-map :joins] "company | employee .company_id")))
    (is (= [{"c_0" {"e_1" ["c_0" nil :has "e_1" nil]}}, [["c_0" "e_1" ["c_0" nil :has "e_1" nil] nil]]]
           (generate [:join-map :joins] "company | employee .employee_id"))) ;; trying with incorrect id
    )
  (testing "Generate ast for `join` where there is ambiguity"
    (is (= [{"e_0" {"d_1" ["e_0" "id" :has "d_1" "created_by"]}} [["e_0" "d_1" ["e_0" "id" :has "d_1" "created_by"] nil]]]
           (generate [:join-map :joins] "employee | document .created_by")))
    (is (= [{"e_0" {"d_1" ["e_0" "id" :has "d_1" "employee_id"]}} [["e_0" "d_1" ["e_0" "id" :has "d_1" "employee_id"] nil]]]
           (generate [:join-map :joins] "employee | document .employee_id"))))

  (testing "Generate ast for `join` using self join"
    ;; By default, we narrow the results
    ;; i.e. we join with the child
    (is (= [{"e_0" {"e_1" ["e_0" "id" :has "e_1" "reports_to"]}} [["e_0" "e_1" ["e_0" "id" :has "e_1" "reports_to"] nil]]]
           (generate [:join-map :joins] "employee | employee")))
    (is (= [{"e_0" {"e_1" ["e_0" "id" :has "e_1" "reports_to"]}} [["e_0" "e_1" ["e_0" "id" :has "e_1" "reports_to"] nil]]]
           (generate [:join-map :joins] "employee | employee .reports_to")))

    ;; However, we can exlicitly saw that the table is a parent using the `^` character
    (is (= [{"e_0" {"e_1" ["e_0" "reports_to" :of "e_1" "id"]}} [["e_0" "e_1" ["e_0" "reports_to" :of "e_1" "id"] nil]]]
           (generate [:join-map :joins] "employee | employee :parent")))
    (is (= [{"e_0" {"e_1" ["e_0" "reports_to" :of "e_1" "id"]}} [["e_0" "e_1" ["e_0" "reports_to" :of "e_1" "id"] nil]]]
           (generate [:join-map :joins] "employee | employee :parent .reports_to"))))

  (testing "Generate ast for `count`"
    (is (= {:column "*"} (generate :count "company | count:"))))

  (testing "Generate ast for `delete`"
    (is (= {:column "id"} (generate :delete "company | delete! .id"))))

  (testing "Generate ast for `group`"
    (is (= [[{:alias "c" :column "status" :index 1} {:symbol "COUNT(1)"}]
            [{:alias "c" :column "status" :index 1}]]
           (generate [:columns :group] "company as c | group: c.status => count"))))

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
           (generate :operation "company | w: name like")))))