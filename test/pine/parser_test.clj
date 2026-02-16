(ns pine.parser-test
  (:require [clojure.test :refer [deftest is testing]]
            [pine.parser :refer [parse-or-fail prettify]]
            [pine.data-types :as dt]))

(defn- p [e]
  (-> e parse-or-fail))

(deftest test-parser

  (testing "Parse `table` expressions"
    (is (= [{:type :table, :value {:table ""}}]                                 (p "")))
    (is (= [{:type :table, :value {:table "user"}}]                             (p " user")))
    (is (= [{:type :table, :value {:table "user" :schema "public"}}]            (p "public.user")))
    (is (= [{:type :table, :value {:table "user" :schema "public"}}]            (p "public.user")))
    (is (= [{:type :table, :value {:table "user" :alias "u"}}]                  (p "user as u")))
    (is (= [{:type :table, :value {:table "user" :schema "public" :alias "u"}}] (p "public.user as u")))
    (is (= [{:type :table, :value {:table "user" :join-column "id"}}]           (p "user .id")))
    (is (= [{:type :table, :value {:schema "public"
                                   :table "user"
                                   :join-column "id"}}]                         (p "public.user .id"))))

  (testing "Parse `table` expressions with directionality"

    ;; table
    (is (= [{:type :table, :value {:table "user" :parent false}}]                 (p "user :child")))
    (is (= [{:type :table, :value {:table "user" :parent true}}]                  (p "user :parent")))

    ;; schema.table
    (is (= [{:type :table, :value {:table "user" :schema "public" :parent false}}] (p "public.user :child")))
    (is (= [{:type :table, :value {:table "user" :schema "public" :parent true}}]  (p "public.user :parent")))

    ;; schema.table alias
    (is (= [{:type :table, :value {:table "user" :schema "public" :parent false :alias "u"}}] (p "public.user as u :child")))
    (is (= [{:type :table, :value {:table "user" :schema "public" :parent true :alias "u"}}]  (p "public.user as u :parent")))

    ;; table .column
    (is (= [{:type :table, :value {:table "user" :parent false :join-column "other_id"}}]     (p "user .other_id :child")))
    (is (= [{:type :table, :value {:table "user" :parent true :join-column "other_id"}}]      (p "user .other_id :parent")))

    ;; schema.table .column
    (is (= [{:type :table, :value {:schema "public" :table "user" :join-column "other_id" :parent false}}]           (p "public.user .other_id :child")))
    (is (= [{:type :table, :value {:schema "public" :table "user" :join-column "other_id" :parent true}}]            (p "public.user .other_id :parent")))

    ;; schema.table alias .column
    (is (= [{:type :table, :value {:schema "public" :table "user" :alias "u" :join-column "other_id" :parent false}}] (p "public.user .other_id as u :child")))
    (is (= [{:type :table, :value {:schema "public" :table "user" :alias "u" :join-column "other_id" :parent true}}]  (p "public.user .other_id as u :parent"))))

  (testing "Parse `table` expressions with join types"
    ;; table
    (is (= [{:type :table, :value {:table "user" :join "LEFT"}}]                 (p "user :left")))
    (is (= [{:type :table, :value {:table "user" :join "RIGHT"}}]                (p "user :right")))

    ;; schema.table
    (is (= [{:type :table, :value {:table "user" :schema "public" :join "LEFT"}}] (p "public.user :left")))
    (is (= [{:type :table, :value {:table "user" :schema "public" :join "RIGHT"}}] (p "public.user :right")))

    ;; schema.table alias
    (is (= [{:type :table, :value {:table "user" :schema "public" :join "LEFT" :alias "u"}}] (p "public.user as u :left")))
    (is (= [{:type :table, :value {:table "user" :schema "public" :join "RIGHT" :alias "u"}}] (p "public.user as u :right")))

    ;; table .column
    (is (= [{:type :table, :value {:table "user" :join "LEFT" :join-column "other_id"}}]     (p "user .other_id :left")))
    (is (= [{:type :table, :value {:table "user" :join "RIGHT" :join-column "other_id"}}]    (p "user .other_id :right")))

    ;; schema.table .column
    (is (= [{:type :table, :value {:schema "public" :table "user" :join-column "other_id" :join "LEFT"}}]           (p "public.user .other_id :left")))
    (is (= [{:type :table, :value {:schema "public" :table "user" :join-column "other_id" :join "RIGHT"}}]          (p "public.user .other_id :right")))

    ;; schema.table alias .column
    (is (= [{:type :table, :value {:schema "public" :table "user" :alias "u" :join-column "other_id" :join "LEFT"}}] (p "public.user .other_id as u :left")))
    (is (= [{:type :table, :value {:schema "public" :table "user" :alias "u" :join-column "other_id" :join "RIGHT"}}] (p "public.user .other_id as u :right"))))

  (testing "Parse `table` expressions with explicit join columns"
    ;; Basic explicit columns
    (is (= [{:type :table, :value {:table "b" :join-left-column "id" :join-right-column "a_id"}}]
           (p "b .a_id = .id")))

    ;; With schema
    (is (= [{:type :table, :value {:table "user" :schema "public" :join-left-column "id" :join-right-column "user_id"}}]
           (p "public.user .user_id = .id")))

    ;; With alias
    (is (= [{:type :table, :value {:table "employee" :alias "e" :join-left-column "company_id" :join-right-column "id"}}]
           (p "employee as e .id = .company_id")))

    ;; Combined with join type
    (is (= [{:type :table, :value {:table "user" :join "LEFT" :join-left-column "user_id" :join-right-column "id"}}]
           (p "user .id = .user_id :left")))

    (is (= [{:type :table, :value {:table "user" :join "RIGHT" :join-left-column "user_id" :join-right-column "id"}}]
           (p "user .id = .user_id :right"))))

  (testing "Parse `from` expressions"
    (is (= [{:type :from, :value {:alias "u"}}] (p "from: u"))))

  (testing "Parse `select-partial` expressions"
    (is (= [{:type :select-partial, :value []}]                        (p "select:")))
    (is (= [{:type :select-partial, :value []}]                        (p "s: ")))
    (is (= [{:type :select-partial, :value [{:column "id"}]}]          (p "s: id,")))
    (is (= [{:type :select-partial, :value [{:column "id"}]}]          (-> "company | s: id," p rest)))
    ;; Not supported yet
    ;; (is (= [{:type :select-partial, :value [{:alias "c" :column ""}]}] (-> "company as c | s: c." p rest)))
    ;; (is (= [{:type :select-partial, :value [{:alias "c" :column ""}]}] (-> "company as c | s: id, c." p rest)))
    )

  (testing "Parse `select` expressions"
    (is (= [{:type :select, :value [{:column  "name"}]}]                              (p "select: name")))
    (is (= [{:type :select, :value [{:alias "u" :column  "name"}]}]                   (p "select: u.name")))
    (is (= [{:type :select, :value [{:column "id"} {:column  "name"}]}]               (p "s: id, name")))
    (is (= [{:type :select, :value [{:column-alias "t_id" :column "id"}]}]            (p "s: id as t_id")))
    (is (= [{:type :select, :value [{:alias "u" :column "id" :column-alias "t_id"}]}] (p "s: u.id as t_id"))))

  (testing "Parse `limit` expressions"
    (is (= [{:type :limit, :value 100}] (p "limit: 100")))
    (is (= [{:type :limit, :value 10}]  (p "l: 10")))
    (is (= [{:type :limit, :value 10}]  (p "10"))))

  (testing "Parse `where` expressions"
    (is (= [{:type :where, :value [(dt/column "name") "=" (dt/string "John Doe")]}]                           (p "where: name='John Doe'")))
    (is (= [{:type :where, :value [(dt/column "name") "=" (dt/string "John Doe")]}]                           (p "w: name='John Doe'")))
    (is (= [{:type :where, :value [(dt/column "name") "=" (dt/string "John Doe")]}]                           (p "name = 'John Doe'")))
    (is (= [{:type :where, :value [(dt/column "name") "=" (dt/string "John Doe")]}]                           (p "name='John Doe'")))
    (is (= [{:type :where, :value [(dt/column "name") "LIKE" (dt/string "John%")]}]                           (p "name like 'John%'")))
    (is (= [{:type :where, :value [(dt/column "name") "NOT LIKE" (dt/string "John%")]}]                       (p "name not like 'John%'")))
    (is (= [{:type :where, :value [(dt/column "name") "ILIKE" (dt/string "john%")]}]                          (p "name ilike 'john%'")))
    (is (= [{:type :where, :value [(dt/column "name") "NOT ILIKE" (dt/string "john%")]}]                      (p "name not ilike 'john%'")))
    (is (= [{:type :where, :value [(dt/column "age") "IS" (dt/pine-symbol "NULL")]}]                          (p "age is null")))
    (is (= [{:type :where, :value [(dt/column "age") "=" (dt/number "24")]}]                                  (p "age = 24")))
    (is (= [{:type :where, :value [(dt/column "age") "!=" (dt/number "24")]}]                                 (p "age != 24")))
    (is (= [{:type :where, :value [(dt/column "name") "=" (dt/column "first_name")]}]                         (p "name = first_name")))
    (is (= [{:type :where, :value [(dt/column "name") "=" (dt/aliased-column "x" "first_name")]}]             (p "name = x.first_name")))
    (is (= [{:type :where, :value [(dt/column "public") "=" (dt/pine-symbol "true")]}]                        (p "public = true")))
    (is (= [{:type :where, :value [(dt/column "public") "=" (dt/pine-symbol "false")]}]                       (p "public = false")))
    (is (= [{:type :where, :value [(dt/column "public") "!=" (dt/pine-symbol "false")]}]                      (p "public != false")))
    (is (= [{:type :where, :value [(dt/column "name") "=" (dt/aliased-column "x" "first_name")]}]             (p "name = x.first_name")))
    (is (= [{:type :where, :value [(dt/aliased-column "x" "name") "=" (dt/aliased-column "x" "first_name")]}] (p "x.name = x.first_name"))))

  (testing "Parse `where` `or` expressions"
    (is (= [{:type :where, :value [(dt/column "name") "=" (dt/string "John Doe")]}]       (p "w: name='John Doe', age=24 "))))

  ;; in progress - the case isn't being processed at the moment
  (testing "Parse `where` expressions with type hinting"
    (is (= [{:type :where, :value [(dt/column "name" "text") "=" (dt/string "John Doe")]}]       (p "w: name = 'John Doe' ::text")))
    (is (= [{:type :where, :value [(dt/column "id" "uuid") "=" (dt/string "123e4567-e89b-12d3-a456-426614174000")]}] (p "w: id = '123e4567-e89b-12d3-a456-426614174000' ::uuid"))))

  (testing "Parse `where` `in` expressions"
    (is (= [{:type :where, :value [(dt/column "age") "IN" [(dt/string "24")]]}]                  (p "age in ('24')")))
    (is (= [{:type :where, :value [(dt/column "age") "IN" [(dt/string "24")]]}]                  (p "age in ('24' ) ")))
    (is (= [{:type :where, :value [(dt/column "age") "IN" [(dt/string "24") (dt/string "36")]]}] (p "age in (  '24' ,'36' )")))
    (is (= [{:type :where, :value [(dt/column "age") "IN" [(dt/string "24") (dt/string "36")]]}] (p "age in ('24' '36')"))))

  (testing "Parse `where` expressions with dates"
    (is (= [{:type :where, :value [(dt/column "created_at") "=" (dt/date "2025-01-01")]}] (p "created_at = '2025-01-01'")))
    (is (= [{:type :where, :value [(dt/column "created_at") "!=" (dt/date "2025-01-01")]}] (p "created_at != '2025-01-01'")))
    (is (= [{:type :where, :value [(dt/column "created_at") ">" (dt/date "2025-01-01")]}] (p "created_at > '2025-01-01'")))
    (is (= [{:type :where, :value [(dt/column "created_at") "<" (dt/date "2025-01-01")]}] (p "created_at < '2025-01-01'"))))

  (testing "Parse `where-partial` expressions"
    (is (= [{:type :where-partial, :value {:complete-conditions [] :partial-condition nil}}] (p "where:")))
    (is (= [{:type :where-partial, :value {:complete-conditions [] :partial-condition nil}}] (p "w: ")))
    (is (= [{:type :where-partial, :value {:complete-conditions [] :partial-condition {:column "id"}}}] (p "w: id")))
    (is (= [{:type :where-partial, :value {:complete-conditions [] :partial-condition {:alias "u", :column "id"}}}] (p "w: u.id")))
    (is (= [{:type :where-partial, :value {:complete-conditions [] :partial-condition {:column "id", :operator :equals}}}] (p "w: id =")))
    (is (= [{:type :where-partial, :value {:complete-conditions [] :partial-condition {:column "id", :operator :like}}}] (p "w: id like"))))

  (testing "Parse `order-partial` expressions"
    (is (= [{:type :order-partial, :value []}]                                  (p "order:")))
    (is (= [{:type :order-partial, :value []}]                                  (p "o: ")))
    (is (= [{:type :order-partial, :value [{:column "id", :direction "DESC"}]}] (p "o: id,"))))

  (testing "Parse `order` expressions"
    (is (= [{:type :order, :value [{:column  "name" :direction "DESC"}]}]            (p "order: name")))
    (is (= [{:type :order, :value [{:column  "name" :direction "DESC"}]}]            (p "order: name desc")))
    (is (= [{:type :order, :value [{:column  "name" :direction "ASC"}]}]            (p "order: name asc")))
    (is (= [{:type :order, :value [{:column  "name" :direction "DESC"}
                                   {:column "created_at" :direction "ASC"}]}]       (p "order: name, created_at asc")))
    ;; Test aliased columns
    (is (= [{:type :order, :value [{:alias "e" :column "name" :direction "DESC"}]}]  (p "order: e.name")))
    (is (= [{:type :order, :value [{:alias "e" :column "name" :direction "ASC"}]}]   (p "order: e.name asc")))
    (is (= [{:type :order, :value [{:alias "e" :column "name" :direction "DESC"}]}]  (p "order: e.name desc")))
    (is (= [{:type :order, :value [{:alias "e" :column "name" :direction "DESC"}
                                   {:alias "e" :column "created_at" :direction "ASC"}]}] (p "order: e.name, e.created_at asc")))

    ;; Test multiple columns (mixed aliased and non-aliased)
    (is (= [{:type :order, :value [{:column "name" :direction "DESC"}
                                   {:column "age" :direction "DESC"}]}] (p "order: name desc, age desc")))
    (is (= [{:type :order, :value [{:alias "e" :column "name" :direction "DESC"}
                                   {:column "title" :direction "ASC"}]}] (p "order: e.name desc, title asc")))
    (is (= [{:type :order, :value [{:column "created_at" :direction "ASC"}
                                   {:alias "u" :column "name" :direction "DESC"}]}] (p "order: created_at asc, u.name desc"))))

  (testing "Parse `count` expressions"
    (is (= [{:type :count, :value {:column "*"}}] (p "count:"))))

  (testing "Parse `delete!` expressions"
    (is (= [{:type :delete-action, :value {:column "id"}}] (p "delete! .id"))))

  (testing "Parse `update!` expressions"
    (is (= [{:type :update-action, :value {:assignments [{:column {:alias nil :column "name"} :value (dt/string "John Doe")}]}}]
           (p "update! name = 'John Doe'")))
    (is (= [{:type :update-action, :value {:assignments [{:column {:alias nil :column "age"} :value (dt/number "25")}]}}]
           (p "update! age = 25")))
    (is (= [{:type :update-action, :value {:assignments [{:column {:alias nil :column "active"} :value (dt/pine-symbol "true")}]}}]
           (p "update! active = true")))
    (is (= [{:type :update-action, :value {:assignments [{:column {:alias nil :column "deleted_at"} :value (dt/pine-symbol "NULL")}]}}]
           (p "update! deleted_at = null")))
    (is (= [{:type :update-action, :value {:assignments [{:column {:alias nil :column "name"} :value (dt/string "John")}
                                                         {:column {:alias nil :column "age"} :value (dt/number "30")}]}}]
           (p "update! name = 'John', age = 30"))))

  (testing "Parse No Operation expressions"
    (is (= [{:value {:table "company"}, :type :table} {:type :delete, :value nil}] (p "company | d:"))))

  (testing "Parse `group` expressions"
    (is (= [{:type :group, :value {:columns [{:column "status"}], :functions ["count"]}}]
           (p "group: status => count")))
    (is (= [{:type :group, :value {:columns [{:column "status"}], :functions ["count", "sum"]}}]
           (p "group: status => count, sum")))
    (is (= [{:type :group, :value {:columns [{:column "name"} {:column "status"}], :functions ["count"]}}]
           (p "group: name, status => count")))

    ;; with aliases
    (is (= [{:type :group, :value {:columns [{:alias "x", :column "status"}], :functions ["count"]}}]
           (p "group: x.status  => count"))))

  (testing "Parse date extraction functions in select"
    (is (= [{:type :select, :value [{:column "created_at", :column-function "year"}]}]
           (p "select: created_at => year")))
    (is (= [{:type :select, :value [{:column "created_at", :column-function "month"}]}]
           (p "select: created_at => month")))
    (is (= [{:type :select, :value [{:column "created_at", :column-function "day"}]}]
           (p "select: created_at => day")))
    (is (= [{:type :select, :value [{:column "created_at", :column-function "week"}]}]
           (p "select: created_at => week")))
    (is (= [{:type :select, :value [{:column "created_at", :column-function "hour"}]}]
           (p "select: created_at => hour")))
    (is (= [{:type :select, :value [{:column "created_at", :column-function "minute"}]}]
           (p "select: created_at => minute")))

    ;; with table alias
    (is (= [{:type :select, :value [{:alias "e", :column "created_at", :column-function "month"}]}]
           (p "select: e.created_at => month")))

    ;; with custom column alias
    (is (= [{:type :select, :value [{:column "created_at", :column-function "month", :column-alias "created_at_month"}]}]
           (p "select: created_at => month as created_at_month")))

    ;; with both table and custom column alias
    (is (= [{:type :select, :value [{:alias "e", :column "created_at", :column-function "year", :column-alias "yr"}]}]
           (p "select: e.created_at => year as yr")))

    ;; mixed with regular columns
    (is (= [{:type :select, :value [{:column "name"} {:column "created_at", :column-function "year"}]}]
           (p "select: name, created_at => year"))))

  (testing "Parse expressions with line comments"
    (is (= [{:type :table, :value {:table "user"}}]
           (p "user -- this is a comment")))
    (is (= [{:type :table, :value {:table "user"}}]
           (p "-- comment before\nuser")))
    (is (= [{:type :where, :value [(dt/column "name") "=" (dt/string "John")]}]
           (p "name = 'John' -- comment after condition")))
    (is (= [{:type :limit, :value 10}]
           (p "limit: 10 -- limit to 10 rows"))))

  (testing "Parse expressions with block comments"
    (is (= [{:type :table, :value {:table "user"}}]
           (p "user /* block comment */")))
    (is (= [{:type :table, :value {:table "user"}}]
           (p "/* comment before */ user")))
    (is (= [{:type :where, :value [(dt/column "name") "=" (dt/string "John")]}]
           (p "name /* comment */ = /* another */ 'John'")))
    (is (= [{:type :table, :value {:table "user"}}]
           (p "user /* multi\nline\ncomment */"))))

  (testing "Parse expressions with mixed comments"
    (is (= [{:type :table, :value {:table "company"}}
            {:type :where, :value [(dt/column "id") "=" (dt/number "1")]}]
           (p "company -- get company\n| where: id = 1 /* by id */")))
    (is (= [{:type :table, :value {:table "user"}}
            {:type :limit, :value 10}]
           (p "-- fetch users\nuser | /* limit results */ limit: 10"))))

  (testing "Comments do not affect strings"
    (is (= [{:type :where, :value [(dt/column "comment") "=" (dt/string "-- not a comment")]}]
           (p "comment = '-- not a comment'")))
    (is (= [{:type :where, :value [(dt/column "text") "=" (dt/string "/* also not a comment */")]}]
           (p "text = '/* also not a comment */'")))
    (is (= [{:type :where, :value [(dt/column "title") "LIKE" (dt/string "A% /* test */")]}]
           (p "title like 'A% /* test */'")))
    (is (= [{:type :where, :value [(dt/column "name") "=" (dt/string "John -- comment in string")]}]
           (p "name = 'John -- comment in string'")))
    (is (= [{:type :where, :value [(dt/column "description") "=" (dt/string "Multi\nline /* block */ comment")]}]
           (p "description = 'Multi\nline /* block */ comment'"))))

  (testing "Parse expressions with consecutive comments"
    (is (= [{:type :table, :value {:table "user"}}]
           (p "-- comment 1\n-- comment 2\nuser")))
    (is (= [{:type :table, :value {:table "user"}}]
           (p "user /* comment 1 */ /* comment 2 */"))))

  (testing "Comments at EOF"
    (is (= [{:type :table, :value {:table "user"}}]
           (p "user --")))
    (is (= [{:type :table, :value {:table "user"}}]
           (p "user -- comment at end")))))

(deftest test-prettify

  (testing "Single operation"
    (is (= {:result "company"
            :operations [{:expression "company" :start 0 :end 7}]}
           (prettify "company"))))

  (testing "Multiple operations"
    (is (= {:result "company\n | where: name = 'Acme'\n | select: id"
            :operations [{:expression "company" :start 0 :end 7}
                         {:expression "where: name = 'Acme'" :start 10 :end 30}
                         {:expression "select: id" :start 33 :end 43}]}
           (prettify "company | where: name = 'Acme' | select: id"))))

  (testing "Pipe inside string value is preserved"
    (is (= {:result "company\n | where: name = 'test | test'"
            :operations [{:expression "company" :start 0 :end 7}
                         {:expression "where: name = 'test | test'" :start 10 :end 37}]}
           (prettify "company | where: name = 'test | test'"))))

  (testing "Extra whitespace is normalized"
    (is (= {:result "company\n | where: id = 1\n | select: name"
            :operations [{:expression "company" :start 0 :end 7}
                         {:expression "where: id = 1" :start 14 :end 27}
                         {:expression "select: name" :start 34 :end 46}]}
           (prettify "company   |   where: id = 1   |   select: name"))))

  (testing "Already formatted expression"
    (is (= {:result "company\n | where: name = 'Acme'"
            :operations [{:expression "company" :start 0 :end 7}
                         {:expression "where: name = 'Acme'" :start 11 :end 31}]}
           (prettify "company\n | where: name = 'Acme'"))))

  (testing "Trailing pipe is preserved"
    (is (= {:result "company\n | "
            :operations [{:expression "company" :start 0 :end 7}
                         {:expression "" :start 9 :end 9}]}
           (prettify "company | ")))
    (is (= {:result "company\n | where: id = 1\n | "
            :operations [{:expression "company" :start 0 :end 7}
                         {:expression "where: id = 1" :start 10 :end 23}
                         {:expression "" :start 25 :end 25}]}
           (prettify "company | where: id = 1 | "))))

  (testing "Incomplete expressions return an error"
    (is (contains? (prettify "company | w: name = 'test |") :error))))

