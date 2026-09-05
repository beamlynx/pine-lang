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
;;
;;   `w`.`department` / `w`.`team` / `w`.`worker` - isolated from the tree
;;   above on purpose (see the foreign-keys comment below): its only job is
;;   to give find-table-paths a case where the shortest route and the
;;   fewest-parent-hops route disagree, which company/employee/document
;;   can't (every route there is already all-child, so shortest-first and
;;   fewest-parent-hops-first always pick the same winner).
;;
;;     +------------------------+
;;     |     w.department       |
;;     |                        |
;;     |       id (PK)          |<---|
;;     |   lead_worker_id (FK)  |----|--+
;;     +------------------------+    |  |
;;              ^                    |  |
;;              |       +-----------------------+
;;              |       |       w.team          |
;;              |       |                       |
;;              |       |       id (PK)         |
;;              +-------|   department_id (FK)  |
;;                      +-----------------------+
;;                          ^
;;                          |       +------------------------+
;;                          |       |       w.worker         |
;;                          |       |                        |
;;                          +-------|      team_id (FK)      |
;;                                  |          id (PK)       |<--+ (lead_worker_id)
;;                                  +------------------------+

(def foreign-keys [["y"  "employee"      "company_id"    "x"  "company"  "id"]
                   ["z"  "document"      "employee_id"   "y"  "employee" "id"]
                   ["z"  "document"      "created_by"    "y"  "employee" "id"]

                   ;; self join
                   ["y"  "employee"      "reports_to"    "y"  "employee" "id"]

                   ["z"  "document"      "company_id"    "x"  "company" "id"]

                   ;; department -> team -> worker is the "real" hierarchy
                   ;; (both hops are child hops, department | ? worker).
                   ["w"  "team"          "department_id" "w"  "department" "id"]
                   ["w"  "worker"        "team_id"       "w"  "team"       "id"]
                   ;; department also keeps a direct, denormalized pointer at
                   ;; its own lead worker - a real-world shortcut FK that
                   ;; happens to point the "wrong" way relative to the
                   ;; hierarchy above: department refers to worker here, so
                   ;; this 1-hop route is a PARENT hop, competing against the
                   ;; 2-hop, all-child route through team.
                   ["w"  "department"    "lead_worker_id" "w"  "worker"     "id"]])

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
              [nil  "customer" "uuid_col"       nil  "uuid"     nil  nil  nil]

              ;; `order` has no FK to `customer` or `user` - both relations are
              ;; only ever found heuristically (by naming convention).
              ;;
              ;; customer_id was mistakenly typed varchar instead of matching
              ;; customer.id's integer type - a genuine cross-family mismatch,
              ;; used to test that a heuristic join between mismatched types
              ;; gets a `::text` cast rather than failing at query time.
              ;;
              ;; user_id is bigint against user.id's integer - a different
              ;; spelling of the same (numeric) family, so it already joins
              ;; fine as-is. Used to test that this does NOT get a cast:
              ;; same-family-but-differently-spelled types shouldn't be
              ;; treated as a mismatch.
              ["public"  "order"    "id"           nil  "integer"            nil  nil  nil]
              ["public"  "order"    "customer_id"  nil  "character varying"  nil  nil  nil]
              ["public"  "order"    "user_id"      nil  "bigint"             nil  nil  nil]
              [nil  "order"    "id"                nil  "integer"            nil  nil  nil]
              [nil  "order"    "customer_id"       nil  "character varying"  nil  nil  nil]
              [nil  "order"    "user_id"           nil  "bigint"             nil  nil  nil]

              ;; `report` has no FK and no column any heuristic could match
              ;; (no `_id`-suffixed column) - a genuinely orphaned table, used
              ;; to test that it still surfaces as a first-position table
              ;; hint even though it has no entry in :refers-to/:referred-by.
              ["public"  "report"   "id"    nil  "integer"            nil  nil  nil]
              ["public"  "report"   "title" nil  "character varying"  nil  nil  nil]

              ;; department/team/worker - see the foreign-keys comment above.
              ["w"  "department"  "id"              nil  "integer"  nil  nil  nil]
              ["w"  "department"  "lead_worker_id"  nil  "integer"  nil  nil  nil]
              ["w"  "team"        "id"              nil  "integer"  nil  nil  nil]
              ["w"  "team"        "department_id"   nil  "integer"  nil  nil  nil]
              ["w"  "worker"      "id"              nil  "integer"  nil  nil  nil]
              ["w"  "worker"      "team_id"         nil  "integer"  nil  nil  nil]])

(def references [foreign-keys columns])
