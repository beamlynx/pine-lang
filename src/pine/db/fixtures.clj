(ns pine.db.fixtures)

;; Following tables exist:
;;
;;   `x`.`company`
;;   `y`.`employee`
;;   `z`.`document`
;;   `public`.`user`
;;   `public`.`customer`
;;
;;     +------------------+
;;     |    x.company     |
;;     |                  |
;;     |     id (PK)      |
;;     +------------------+
;;              ^
;;              |       +-----------------------+
;;              |       |     y.employee        |
;;              |       |                       |
;;              |       |       id (PK)         |
;;              +-------|      company_id (FK)  |<---|
;;              |       |      reports_to (FK)  |----|
;;              |       +-----------------------+
;;              |           ^
;;              |           |       +------------------------+
;;              |           |       |       z.document       |
;;              |           |       |                        |
;;              |           |       |      id (PK)           |
;;              |           |-------|     employee_id (FK)   |
;;              |           +-------|     created_by  (FK)   |
;;              +-------------------|     company_id (FK)    |
;;                                  +------------------------+
;;
;;              +------------------+
;;              |   public.user    |
;;              |                  |
;;              |     id (PK)      |
;;              +------------------+
;;
;;              +------------------+
;;              |  public.customer |
;;              |                  |
;;              |     id (PK)      |
;;              +------------------+

(def foreign-keys [["y"  "employee"      "company_id"    "x"  "company"  "id"]
                   ["z"  "document"      "employee_id"   "y"  "employee" "id"]
                   ["z"  "document"      "created_by"    "y"  "employee" "id"]

                   ;; self join
                   ["y"  "employee"      "reports_to"    "y"  "employee" "id"]

                   ["z"  "document"      "company_id"    "x"  "company" "id"]])

;; schema table col pos type len nullable default
(def columns [["x"  "company"   "id"           nil  "integer"  nil  nil  nil]
              ["x"  "company"   "created_at"   nil  "timestamp"  nil  nil  nil]
              ["y"  "employee"  "id"           nil  "integer"  nil  nil  nil]
              ["y"  "employee"  "company_id"   nil  "integer"  nil  nil  nil]
              ["y"  "employee"  "reports_to"   nil  "integer"  nil  nil  nil]
              ["z"  "document"  "id"           nil  "integer"  nil  nil  nil]
              ["z"  "document"  "employee_id"  nil  "integer"  nil  nil  nil]
              ["z"  "document"  "created_by"   nil  "integer"  nil  nil  nil]
              ["z"  "document"  "company_id"   nil  "integer"  nil  nil  nil]
              ;; Add user and customer tables for tests
              ["public"  "user"     "id"        nil  "integer"  nil  nil  nil]
              ["public"  "customer" "id"        nil  "integer"  nil  nil  nil]
              ["public"  "customer" "data"      nil  "jsonb"    nil  nil  nil]
              ["public"  "customer" "uuid_col"  nil  "uuid"     nil  nil  nil]
              ;; Also add without schema for tests that don't specify schema
              [nil  "user"     "id"             nil  "integer"  nil  nil  nil]
              [nil  "customer" "id"             nil  "integer"  nil  nil  nil]
              [nil  "customer" "data"           nil  "jsonb"    nil  nil  nil]
              [nil  "customer" "uuid_col"       nil  "uuid"     nil  nil  nil]])

(def references [foreign-keys columns])
