(ns pine.hints-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [pine.parser :as parser]
   [pine.ast.main :as ast]))

(defn- gen
  "Helper function to generate and get the relevant part in the ast"
  ([expression]
   (gen expression nil))
  ([expression cursor]
   (-> expression
       parser/parse
       :result
       (ast/generate :test expression cursor)
       :hints)))

(defn- gen-with-variables
  "Evaluate expressions sequentially, threading variables. Returns :hints from the last expression."
  [expressions]
  (let [{:keys [last-hints]}
        (reduce (fn [{:keys [variables]} expr]
                  (let [{:keys [result]} (parser/parse expr)
                        state (ast/generate result :test expr nil variables)]
                    {:variables (merge variables (:pending-assignments state))
                     :last-hints (:hints state)}))
                {:variables {} :last-hints nil}
                expressions)]
    last-hints))

(deftest test-hints
  (testing "Generate hints"
    (is (= [{:schema "x", :table "company"
             :pine "x.company"}]
           (-> "co" gen :table)))

    (is (= [{:schema "y", :table "employee" :column "company_id" :related-column "id" :parent false
             :resolution "fk" :pine "y.employee .company_id"}
            {:schema "z", :table "document", :column "company_id" :related-column "id", :parent false
             :resolution "fk" :pine "z.document .company_id"}]
           (-> "company | e" gen :table)))

    (is (= [{:schema "x", :table "company" :column "id" :related-column "company_id" :parent true
             :resolution "fk" :pine "x.company .company_id :parent"}]
           (-> "employee | co" gen :table)))

    (is (= []
           (-> "company as c | s: id" gen :table)))

;; The following shouldn't generate any hint but it does
    ;;
    ;; (is (= {:table []}
    ;;        (gen "company as c")))
    )

  (testing "A table with no FK and no heuristic relation still gets a first-position hint"
    (is (= [{:schema "public", :table "report"
             :pine "public.report"}]
           (-> "repo" gen :table))))

  (testing "Generate hints in ambiguity"
    (is (= [{:schema "z",
             :table "document"
             :column "employee_id"
             :related-column "id"
             :parent false
             :resolution "fk"
             :pine "z.document .employee_id"}
            {:schema "z"
             :table "document"
             :column "created_by"
             :related-column "id"
             :parent false
             :resolution "fk"
             :pine "z.document .created_by"}]
           (-> "employee | doc" gen :table))))

  (testing "Generate hints when direction is specified"
    (is (= [{:schema "y"
             :table "employee"
             :column "id"
             :related-column "reports_to"
             :parent true
             :resolution "fk"
             :pine "y.employee .reports_to :parent"}
            {:schema "y"
             :table "employee"
             :column "reports_to"
             :related-column "id"
             :parent false
             :resolution "fk"
             :pine "y.employee .reports_to"}]
           (-> "employee | employee" gen :table)))
    (is (= [{:schema "y"
             :table "employee"
             :column "id"
             :related-column "reports_to"
             :parent true
             :resolution "fk"
             :pine "y.employee .reports_to :parent"}]
           (-> "employee | employee :parent" gen :table))))

  (testing "Generate `select` hints with columns specified"
    (is (= []
           (-> "x.company | s: does_not_exist" gen :select)))
    (is (= [{:column "id" :alias "c_0"}]
           (-> "x.company | s: i" gen :select)))
    (is (= ["id" "company_id"] ;;  "reports_to" is not returned
           (->> "y.employee | s: id" gen :select (map :column))))
    (is (= ["id" "company_id" "reports_to"]
           (->> "y.employee as e | s: e.*" gen :select (map :column)))))

  (testing "Generate `select-partial` hints"
    (is (= [{:column "id" :alias "c_0"} {:column "created_at" :alias "c_0"}] (->  "company    | s:"                      gen :select)))
    (is (= [{:column "id" :alias "c_0"} {:column "created_at" :alias "c_0"}] (->  "x.company  | s:"                      gen :select)))
    (is (= ["id" "company_id" "reports_to"]                                 (->> "y.employee | s:"                      gen :select (map :column))))
    (is (= ["reports_to"]                                                    (->> "y.employee | s: id, company_id,"      gen :select (map :column))))
    (is (= ["id" "company_id" "reports_to"]                                 (->> "company | s: id | employee | s: "     gen :select (map :column))))
    (is (= ["company_id" "reports_to"]                                      (->> "company | s: id | employee | s: id, " gen :select (map :column))))

    ;; Cross-table: after selecting from company, next slot defaults to current context (employee)
    (is (= ["id" "company_id" "reports_to"]
           (->> "x.company as c | y.employee as e | s: c.id,"  gen :select (map :column))))

    ;; Alias-dot partial: s: e. should show all columns for alias e
    (is (= ["id" "company_id" "reports_to"]
           (->> "x.company as c | y.employee as e | s: e."  gen :select (map :column))))

    ;; Alias-dot after completed column: s: id, e. should show all columns for alias e
    (is (= ["id" "company_id" "reports_to"]
           (->> "employee as e | company | s: id, e." gen :select (map :column))))

    ;; Alias-dot excludes already-selected columns for that alias: s: e.id, e. omits id
    (is (= ["company_id" "reports_to"]
           (->> "x.company as c | y.employee as e | s: e.id, e." gen :select (map :column)))))

  (testing "Generate `select-partial` hints after an un-sealed GROUP checkpoint"
    ;; group: c.name hasn't been sealed into a CTE yet (no table op follows before
    ;; s:), but the columns available for hints should be the group's own output
    ;; (name, count), not the underlying employee table's raw schema.
    (is (= ["name" "count"]
           (->> "company as c | employee .company_id | group: c.name | s: " gen :select (map :column))))
    ;; Same, with an explicit |= assign before the un-sealed s: — still same-expression,
    ;; still not sealed into a CTE until a table op follows.
    (is (= ["name" "count"]
           (->> "company as c | employee .company_id | group: c.name |= x | s: " gen :select (map :column)))))

  (testing "Generate `order-partial` hints"
    (is (= [{:column "id" :alias "c_0"} {:column "created_at" :alias "c_0"}] (->  "company | o:"         gen :order)))
    (is (= [{:column "created_at" :alias "c_0"}]                             (->  "company | o: id,"     gen :order)))
    (is (= [{:column "id" :alias "c_0"} {:column "created_at" :alias "c_0"}] (->  "company | s: id | o:" gen :order)))
    ;; Alias-dot partial: o: e. should show all columns for alias e
    (is (= ["id" "company_id" "reports_to"]
           (->> "x.company as c | y.employee as e | o: e." gen :order (map :column)))))

  (testing "Generate `where-partial` hints"
    (is (= [{:column "id" :alias "c_0"} {:column "created_at" :alias "c_0"}] (->  "company | where:"       gen :where)))
    (is (= [{:column "id" :alias "c_0"} {:column "created_at" :alias "c_0"}] (->  "company | w:"           gen :where)))
    (is (= ["id" "company_id" "reports_to"]                                 (->> "y.employee | w:"        gen :where (map :column))))
    (is (= ["id" "company_id" "reports_to"]                                 (->> "y.employee | where:"    gen :where (map :column))))
    (is (= [{:column "id" :alias "c_0"} {:column "created_at" :alias "c_0"}] (->  "company | s: id | w:"   gen :where)))

    ;; Test partial column filtering
    (is (= [{:column "id" :alias "c_0"}]     (->  "company | w: i"         gen :where)))
    (is (= []                                (->  "company | w: xyz"       gen :where)))
    (is (= ["id" "company_id"]               (->> "y.employee | w: id"     gen :where (map :column))))

    ;; Explicit alias: w: e.col should use alias "e" for column lookup, not the current context
    (is (= [{:column "company_id" :alias "e"}]
           (-> "y.employee as e | x.company as c | w: e.company_id" gen :where)))
    (is (= [{:column "id" :alias "c"}]
           (-> "y.employee as e | x.company as c | w: c.id"         gen :where)))
    ;; Alias-dot partial: w: e. should show all columns for alias e
    (is (= ["id" "company_id" "reports_to"]
           (->> "x.company as c | y.employee as e | w: e." gen :where (map :column))))

    ;; Complete condition then alias-dot: w: id = 1, e. shows all columns for alias e
    (is (= ["id" "company_id" "reports_to"]
           (->> "x.company as c | y.employee as e | w: id = 1, e." gen :where (map :column))))

    ;; How to auto-complete the right hand side? Values or other columns?
    ;; Right now it shows the same hints as the left hand side
    ;; (is (= [{:column "id" :alias "c_0"}]     (->  "company | w: id ="      gen :where)))
    ;; (is (= ["reports_to"  "company_id" "id"] (->> "y.employee | w: id ="   gen :where (map :column))))
    )

  (testing "Generate `update-partial` hints"
    (is (= [{:column "id" :alias "c_0"} {:column "created_at" :alias "c_0"}]
           (-> "company | u!" gen :update)))
    ;; Explicit alias in update: u! e.col should use alias "e" for column lookup, not current context
    (is (= [{:column "company_id" :alias "e"}]
           (-> "y.employee as e | x.company as c | u! e.company_id" gen :update)))
    (is (= [{:column "created_at" :alias "c_0"}]
           (-> "company | u! id = '1'," gen :update)))
    (is (= [{:column "id" :alias "c_0"}]
           (-> "company | u! i" gen :update)))
    ;; Alias-dot partial: u! e. should show all columns for alias e
    (is (= ["id" "company_id" "reports_to"]
           (->> "x.company as c | y.employee as e | u! e." gen :update (map :column))))

    ;; Prior assignment then alias-dot: u! id = '1', e. excludes already-assigned id
    (is (= ["company_id" "reports_to"]
           (->> "x.company as c | y.employee as e | u! id = '1', e." gen :update (map :column)))))

  (testing "Generate hints with cursor position"
    ;; Basic cursor truncation test - cursor at "company | s: " should show select hints for company
    (is (= [{:column "id" :alias "c_0"} {:column "created_at" :alias "c_0"}]
           (-> (gen "company | s: id | employee | s: " {:line 0 :character 13}) :select)))

    ;; Cursor at different positions in single line expression
    (is (= [{:column "id" :alias "c_0"}]
           (-> (gen "company | s: id | employee | s: " {:line 0 :character 14}) :select)))

    ;; Cursor at "company | s: id | employee | s:" should show all columns
    (is (= ["id" "company_id" "reports_to"]
           (->> (gen "company | s: id | employee | s: id, " {:line 0 :character 31})
                :select
                (map :column))))

    ;; Multi-line expression with cursor on first line (should show table hints)
    (is (= [{:schema "x" :table "company" :pine "x.company"}]
           (-> (gen "company\n | s: id | employee | s: " {:line 0 :character 7}) :table)))

    ;; Multi-line expression with cursor on second line
    (is (= [{:column "id" :alias "c_0"} {:column "created_at" :alias "c_0"}]
           (-> (gen "company\n | s: " {:line 1 :character 6}) :select)))

    ;; Edge case: cursor at start (should show select hints for company)
    (is (= [{:column "id" :alias "c_0"}]
           (-> (gen "company | s: id" {:line 0 :character 14}) :select)))

    ;; Edge case: cursor at end should behave like no cursor
    (is (= ["id" "company_id" "reports_to"]
           (->> (gen "company | s: id | employee | s: " {:line 0 :character 100})
                :select
                (map :column)))))

  (testing "Variable relation hints"
    ;; mytest = employee (child); company | my should suggest mytest
    (is (= [{:schema nil :table "mytest" :column "company_id" :related-column "id" :parent false
             :resolution "fk" :pine "mytest .company_id"}]
           (-> (gen-with-variables ["employee |= mytest" "company | my"])
               :table)))

    ;; mytest = company (parent); employee | my should suggest mytest
    (is (= [{:schema nil :table "mytest" :column "id" :related-column "company_id" :parent true
             :resolution "fk" :pine "mytest .company_id :parent"}]
           (-> (gen-with-variables ["company |= mytest" "employee | my"])
               :table)))

    ;; same-source variables: var_x and var_y both wrap company;
    ;; when typing "var_x | var", var_y should appear (partial match, unambiguous token).
    ;; Both sides are always "id" for a synthetic join - no ambiguity to
    ;; disambiguate, so the pine expression stays suffix-free.
    (is (= [{:schema nil :table "var_y" :column "id" :related-column "id" :parent false
             :resolution "synthetic" :pine "var_y"}]
           (-> (gen-with-variables ["customer |= var_x" "customer |= var_y" "var_x | var"])
               :table))))

  (testing "Variable relation hints reflect an explicitly-aliased id column"
    ;; x = company with id renamed to tmp_id; x | emp should suggest employee via
    ;; tmp_id (x's own actual column), not the underlying raw "id".
    (is (= [{:schema "y" :table "employee" :column "company_id" :related-column "tmp_id" :parent false
             :resolution "fk" :pine "y.employee .company_id"}]
           (-> (gen-with-variables ["company | s: id as tmp_id |= x" "x | emp"])
               :table)))

    ;; Reverse direction: employee already relates to company, so employee | x
    ;; must resolve on x's own tmp_id too, not id.
    (is (= [{:schema nil :table "x" :column "tmp_id" :related-column "company_id" :parent true
             :resolution "fk" :pine "x .company_id :parent"}]
           (->> (gen-with-variables ["company | s: id as tmp_id |= x" "employee | "])
                :table
                (filter #(= "x" (:table %)))))))

  (testing "Same-source join hint also suggests the real table itself, not just other variables"
    ;; x wraps company via an explicitly-renamed id (c_id) - company itself is
    ;; just as valid a same-source candidate from x as another variable
    ;; wrapping company would be, since it's the same underlying table.
    (is (= [{:schema nil :table "company" :column "id" :related-column "c_id" :parent false
             :resolution "synthetic" :pine "company"}]
           (-> (gen-with-variables ["company | s: id as c_id |= x" "x | co"])
               :table))))

  (testing "A table is only a valid join source through a variable if its own id survives"
    ;; x explicitly selects only name — Pine doesn't add an id on its own, so
    ;; company's id is nowhere in x's actual CTE output. employee must NOT be
    ;; suggested.
    (is (= []
           (-> (gen-with-variables ["company as c | s: name |= x" "x | emp"])
               :table)))

    ;; Same table, but id is explicitly selected this time — now it's present,
    ;; and employee is correctly suggested.
    (is (= ["employee"]
           (->> (gen-with-variables ["company as c | s: id, name |= x" "x | emp"])
                :table
                (map :table))))

    ;; Same rule applies to GROUP: grouping by a non-id column loses company's
    ;; id just the same — employee must NOT be suggested.
    (is (= []
           (-> (gen-with-variables ["company as c | employee .company_id | group: c.name |= x" "x | doc"])
               :table)))

    ;; ...but grouping by id (alongside other columns) preserves it, same as s: id, name.
    (is (= ["document"]
           (->> (gen-with-variables ["company as c | employee .company_id | group: c.id, c.name |= x" "x | doc"])
                :table
                (map :table)))))

  (testing "A checkpoint (l:/group:) that seals into an anonymous CTE still surfaces FK hints unrelated to id"
    ;; Regression: `employee | s: company_id | l: 10 | ` used to lose ALL
    ;; hints once l: sealed the selection into an anonymous CTE, even though
    ;; the company FK hint only needs company_id (not id) to be reachable -
    ;; only the synthetic self-join genuinely needs id.
    (is (= [{:schema "x" :table "company" :column "id" :related-column "company_id" :parent true
             :resolution "fk" :pine "x.company .company_id :parent"}]
           (-> "employee | s: company_id | l: 10 | " gen :table)))

    ;; Selecting id alongside company_id additionally unlocks the synthetic
    ;; self-join (employee = employee), since id now actually survives -
    ;; without collapsing the company FK hint that was already reachable.
    (is (= [{:schema "x" :table "company" :column "id" :related-column "company_id" :parent true
             :resolution "fk" :pine "x.company .company_id :parent"}
            {:schema nil :table "employee" :column "id" :related-column "id" :parent false
             :resolution "synthetic" :pine "employee"}]
           (->> (-> "employee | s: id, company_id | l: 10 | " gen :table)
                (filter #(or (= (:resolution %) "synthetic") (= (:table %) "company"))))))))

