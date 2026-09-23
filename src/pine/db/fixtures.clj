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
;;   `w`.`department` / `w`.`team` / `w`.`worker` / `w`.`shift` - isolated
;;   from the tree above on purpose (see the foreign-keys comment below):
;;   its only job is to give find-table-paths a case where a shorter route
;;   that changes direction (parent hop then child hop, or vice versa)
;;   competes against a longer route that stays in one direction the whole
;;   way, which company/employee/document can't (every route there already
;;   stays in one direction, so shortest-first and fewest-direction-changes
;;   always pick the same winner).
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
;;                                      ^
;;                                      |       +------------------------+
;;                                      |       |       w.shift          |
;;                                      |       |                        |
;;                                      +-------|      worker_id (FK)    |
;;                                              |          id (PK)       |
;;                                              +------------------------+

;;   A composite foreign key (schema "k"), kept away from every other
;;   fixture table so it can't move an existing assertion:
;;
;;     +---------------------------+          +----------------------+
;;     |        k.case_ref         |          |       k.case         |
;;     |                           |          |                      |
;;     |  case_id    (FK, part 1)  |--------->|   id        (PK)     |
;;     |  search_id  (FK, part 2)  |--------->|   search_id          |
;;     +---------------------------+          +----------------------+
;;
;;   Both columns are one constraint. Matching on `case_id` alone happens
;;   to be right here (case.id is unique on its own); matching on
;;   `search_id` alone is not (several cases share a search), which is
;;   what made the old one-relation-per-column shape silently wrong.

;; schema table col f-schema f-table f-col constraint-name position
(def foreign-keys [["y"  "employee"      "company_id"    "x"  "company"  "id"   "employee_company_fkey"    1]
                   ["z"  "document"      "employee_id"   "y"  "employee" "id"   "document_employee_fkey"   1]
                   ["z"  "document"      "created_by"    "y"  "employee" "id"   "document_created_by_fkey" 1]

                   ;; self join
                   ["y"  "employee"      "reports_to"    "y"  "employee" "id"   "employee_reports_to_fkey" 1]

                   ["z"  "document"      "company_id"    "x"  "company" "id"    "document_company_fkey"    1]

                   ;; One constraint, two columns - see the diagram above.
                   ["k"  "case_ref"      "case_id"       "k"  "case"    "id"        "case_ref_case_fkey"   1]
                   ["k"  "case_ref"      "search_id"     "k"  "case"    "search_id" "case_ref_case_fkey"   2]

                   ;; department -> team -> worker -> shift is the "real"
                   ;; hierarchy (every hop here is a child hop).
                   ["w"  "team"          "department_id" "w"  "department" "id"]
                   ["w"  "worker"        "team_id"       "w"  "team"       "id"]
                   ["w"  "shift"         "worker_id"     "w"  "worker"     "id"]
                   ;; department also keeps a direct, denormalized pointer at
                   ;; its own lead worker - a real-world shortcut FK that
                   ;; happens to point the "wrong" way relative to the
                   ;; hierarchy above (department refers to worker here, a
                   ;; PARENT hop). `department | ? shift` can reach shift
                   ;; either via this shortcut then back down to shift (a
                   ;; parent hop followed by a child hop - one direction
                   ;; change) or via team/worker (three child hops, zero
                   ;; direction changes) - the longer, direction-pure route
                   ;; should win.
                   ;; Two keys between the SAME pair of tables that share a
                   ;; column - ordinary wherever two roles point at the same
                   ;; table and something like a tenant column is in both
                   ;; keys. `.note_id` belongs to both, so it names whichever
                   ;; was indexed first; naming a second column is what gets
                   ;; at the other one.
                   ["k"  "note_ref"      "note_id"        "k"  "note"    "id"        "note_ref_primary_fkey" 1]
                   ["k"  "note_ref"      "search_id"      "k"  "note"    "search_id" "note_ref_primary_fkey" 2]
                   ["k"  "note_ref"      "note_id"        "k"  "note"    "id"        "note_ref_related_fkey" 1]
                   ["k"  "note_ref"      "other_id"       "k"  "note"    "other_id"  "note_ref_related_fkey" 2]

                   ;;
                   ;; Deliberately left without a constraint name, unlike
                   ;; every row above: a dialect that doesn't report one has
                   ;; to keep indexing each row as its own single-column
                   ;; relation, rather than being grouped with anything else.
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

              ;; department/team/worker/shift - see the foreign-keys comment above.
              ["w"  "department"  "id"              nil  "integer"  nil  nil  nil]
              ["w"  "department"  "lead_worker_id"  nil  "integer"  nil  nil  nil]
              ["w"  "team"        "id"              nil  "integer"  nil  nil  nil]
              ["w"  "team"        "department_id"   nil  "integer"  nil  nil  nil]
              ["w"  "worker"      "id"              nil  "integer"  nil  nil  nil]
              ["w"  "worker"      "team_id"         nil  "integer"  nil  nil  nil]
              ["w"  "shift"       "id"              nil  "integer"  nil  nil  nil]
              ["w"  "shift"       "worker_id"       nil  "integer"  nil  nil  nil]

              ;; k.case / k.case_ref - the composite foreign key, see the
              ;; foreign-keys comment above. case.search_id is NOT unique,
              ;; which is the whole point of the pair.
              ["k"  "case"      "id"          nil  "integer"  nil  nil  nil]
              ["k"  "case"      "search_id"   nil  "integer"  nil  nil  nil]
              ["k"  "case_ref"  "id"          nil  "integer"  nil  nil  nil]
              ["k"  "case_ref"  "case_id"     nil  "integer"  nil  nil  nil]
              ["k"  "case_ref"  "search_id"   nil  "integer"  nil  nil  nil]

              ;; k.note / k.note_ref - two overlapping composite keys, see the
              ;; foreign-keys comment above.
              ["k"  "note"      "id"          nil  "integer"  nil  nil  nil]
              ["k"  "note"      "search_id"   nil  "integer"  nil  nil  nil]
              ["k"  "note"      "other_id"    nil  "integer"  nil  nil  nil]
              ["k"  "note_ref"  "id"          nil  "integer"  nil  nil  nil]
              ["k"  "note_ref"  "note_id"     nil  "integer"  nil  nil  nil]
              ["k"  "note_ref"  "search_id"   nil  "integer"  nil  nil  nil]
              ["k"  "note_ref"  "other_id"    nil  "integer"  nil  nil  nil]

              ;; `product` exists only to exercise data_types.clj's MySQL
              ;; branches (json/datetime/tinyint) in eval-test's MySQL
              ;; deftest - additive, isolated from every other table (no FK,
              ;; and no column ending in `_id`/`Id`, so the heuristic
              ;; relation detector doesn't invent spurious relations from it).
              ["public"  "product"  "id"         nil  "integer"  nil  nil  nil]
              ["public"  "product"  "config"     nil  "json"     nil  nil  nil]
              ["public"  "product"  "released"   nil  "datetime" nil  nil  nil]
              ["public"  "product"  "active"     nil  "tinyint"  nil  nil  nil]
              [nil  "product"  "id"              nil  "integer"  nil  nil  nil]
              [nil  "product"  "config"          nil  "json"     nil  nil  nil]
              [nil  "product"  "released"        nil  "datetime" nil  nil  nil]
              [nil  "product"  "active"          nil  "tinyint"  nil  nil  nil]])

(def references [foreign-keys columns])
