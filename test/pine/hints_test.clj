(ns pine.hints-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [pine.parser :as parser]
   [pine.ast.main :as ast]))

(defn- gen
  "Helper function to generate and get the relevant part in the ast"
  [expression]
  (-> expression
      parser/parse
      :result
      (ast/generate :test)
      :hints))

(deftest test-hints
  (testing "Generate hints"
    (is (= [{:schema "x", :table "company"
             :pine "x.company"}]
           (-> "co" gen :table)))

    (is (= [{:schema "y", :table "employee" :column "company_id" :parent false
             :pine "y.employee .company_id"}
            {:schema "z", :table "document", :column "company_id", :parent false,
             :pine "z.document .company_id"}]
           (-> "company | e" gen :table)))

    (is (= [{:schema "x", :table "company" :column "company_id" :parent true
             :pine "x.company .company_id :parent"}]
           (-> "employee | co" gen :table)))

    (is (= []
           (-> "company as c | s: id" gen :table)))

;; The following shouldn't generate any hint but it does
    ;;
    ;; (is (= {:table []}
    ;;        (gen "company as c")))
    )

  (testing "Generate hints in ambiguity"
    (is (= [{:schema "z",
             :table "document"
             :column "employee_id"
             :parent false
             :pine "z.document .employee_id"}
            {:schema "z"
             :table "document"
             :column "created_by"
             :parent false
             :pine "z.document .created_by"}]
           (-> "employee | doc" gen :table))))

  (testing "Generate hints when direction is specified"
    (is (= [{:schema "y"
             :table "employee"
             :column "reports_to"
             :parent true
             :pine "y.employee .reports_to :parent"}
            {:schema "y"
             :table "employee"
             :column "reports_to"
             :parent false
             :pine "y.employee .reports_to"}]
           (-> "employee | employee" gen :table)))
    (is (= [{:schema "y"
             :table "employee"
             :column "reports_to"
             :parent true
             :pine "y.employee .reports_to :parent"}]
           (-> "employee | employee :parent" gen :table))))

  (testing "Generate `select` hints with columns specified"
    (is (= []
           (-> "x.company | s: does_not_exist" gen :select)))
    (is (= [{:column "id" :alias "c_0"}]
           (-> "x.company | s: i" gen :select)))
    (is (= ["company_id" "id"] ;;  "reports_to" is not returned
           (->> "y.employee | s: id" gen :select (map :column))))
    (is (= ["reports_to" "company_id" "id"]
           (->> "y.employee as e | s: e.*" gen :select (map :column)))))

  (testing "Generate `select-partial` hints"
    (is (= [{:column "created_at" :alias "c_0"} {:column "id" :alias "c_0"}] (->  "company    | s:"                      gen :select)))
    (is (= [{:column "created_at" :alias "c_0"} {:column "id" :alias "c_0"}] (->  "x.company  | s:"                      gen :select)))
    (is (= ["reports_to"  "company_id" "id"]                                 (->> "y.employee | s:"                      gen :select (map :column))))
    (is (= ["reports_to"]                                                    (->> "y.employee | s: id, company_id,"      gen :select (map :column))))
    (is (= ["reports_to"  "company_id" "id"]                                 (->> "company | s: id | employee | s: "     gen :select (map :column))))
    (is (= ["reports_to"  "company_id"]                                      (->> "company | s: id | employee | s: id, " gen :select (map :column))))

    ;; The following doesn't get parsed at the moment
    ;; We need to update the pine.bnf to support the syntax
    ;;
    ;; (is (= ["reports_to"  "company_id" "id"] (->> "employee as e | company | s: id, e."          gen :select (map :column))))
    )

  (testing "Generate `order-partial` hints"
    (is (= [{:column "created_at" :alias "c_0"} {:column "id" :alias "c_0"}] (->  "company | o:"         gen :order)))
    (is (= [{:column "created_at" :alias "c_0"}]                             (->  "company | o: id,"     gen :order)))
    (is (= [{:column "created_at" :alias "c_0"} {:column "id" :alias "c_0"}] (->  "company | s: id | o:" gen :order))))

  (testing "Generate `where-partial` hints"
    (is (= [{:column "created_at" :alias "c_0"} {:column "id" :alias "c_0"}] (->  "company | where:"       gen :where)))
    (is (= [{:column "created_at" :alias "c_0"} {:column "id" :alias "c_0"}] (->  "company | w:"           gen :where)))
    (is (= ["reports_to"  "company_id" "id"]                                 (->> "y.employee | w:"        gen :where (map :column))))
    (is (= ["reports_to"  "company_id" "id"]                                 (->> "y.employee | where:"    gen :where (map :column))))
    (is (= [{:column "created_at" :alias "c_0"} {:column "id" :alias "c_0"}] (->  "company | s: id | w:"   gen :where)))

    ;; Test partial column filtering
    (is (= [{:column "id" :alias "c_0"}]     (->  "company | w: i"         gen :where)))
    (is (= []                                (->  "company | w: xyz"       gen :where)))
    (is (= ["company_id" "id"]               (->> "y.employee | w: id"     gen :where (map :column))))

    ;; How to auto-complete the right hand side? Values or other columns?
    ;; Right now it shows the same hints as the left hand side
    ;; (is (= [{:column "id" :alias "c_0"}]     (->  "company | w: id ="      gen :where)))
    ;; (is (= ["reports_to"  "company_id" "id"] (->> "y.employee | w: id ="   gen :where (map :column))))
    ))
