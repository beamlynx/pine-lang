(ns pine.db.dates-test
  "Periods of days in where:, today(), yesterday(), tomorrow(), this_week()
  and this_month(), on real SQLite, MySQL and Postgres databases. See
  pine.db.engines, and docs/computed-values.md."
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.test :refer [deftest is testing]]
            [pine.db.engines :as e]))

(def ^:private fixture
  ["CREATE TABLE event (id INTEGER PRIMARY KEY, name {text} NOT NULL, created_at {timestamp} NOT NULL)"])

(def ^:private events
  "Each event's name, and its time in minutes from the start of today. The
  edges of a day are the point: the last minute before it, its first and
  its last."
  [["yesterday 23:00" -60]
   ["today 00:00" 0]
   ["today 12:00" 720]
   ["today 23:59" 1439]
   ["tomorrow 00:00" 1440]
   ["tomorrow 12:00" 2160]
   ["in two days" 2880]
   ["a week ago" -10080]
   ["31 days ago" -44640]])

(defn- at-minute
  "SQL for the start of the database's today plus `minutes`, written in the
  engine's own SQL rather than Pine's, so the test doesn't check Pine's SQL
  against itself."
  [engine minutes]
  (case engine
    :postgres (str "CURRENT_DATE + " minutes " * INTERVAL '1 minute'")
    :mysql (str "CURDATE() + INTERVAL " minutes " MINUTE")
    :sqlite (str "datetime('now', 'start of day', '" minutes " minutes')")))

(defn- insert-events! [{:keys [engine spec]}]
  (doseq [[i [name minutes]] (map-indexed vector events)]
    (jdbc/execute! spec [(str "INSERT INTO event VALUES (" i ", '" name "', " (at-minute engine minutes) ")")])))

(defn- names [db condition]
  (set (e/column db (str "event | where: " condition " | select: name"))))

(deftest test-periods
  ;; A run that crosses midnight can fail: the events are placed on one day,
  ;; and compared on the next.
  (e/on-every-engine
   fixture
   (fn [db]
     (insert-events! db)

     (testing "= is the whole day, whatever the time"
       (is (= #{"today 00:00" "today 12:00" "today 23:59"} (names db "created_at = today()")))
       (is (= #{"yesterday 23:00"} (names db "created_at = yesterday()")))
       (is (= #{"tomorrow 00:00" "tomorrow 12:00"} (names db "created_at = tomorrow()"))))

     (testing "!= is any other day"
       (is (= #{"yesterday 23:00" "tomorrow 00:00" "tomorrow 12:00" "in two days" "a week ago" "31 days ago"}
              (names db "created_at != today()"))))

     (testing "> is after the day ends, < before it starts"
       (is (= #{"tomorrow 00:00" "tomorrow 12:00" "in two days"} (names db "created_at > today()")))
       (is (= #{"yesterday 23:00" "a week ago" "31 days ago"} (names db "created_at < today()"))))

     (testing ">= and <= take the day in"
       (is (= #{"today 00:00" "today 12:00" "today 23:59" "tomorrow 00:00" "tomorrow 12:00" "in two days"}
              (names db "created_at >= today()")))
       (is (= #{"yesterday 23:00" "today 00:00" "today 12:00" "today 23:59" "a week ago" "31 days ago"}
              (names db "created_at <= today()"))))

     (testing "this_week() starts on Monday: today is in it, a week ago never is"
       (let [week (names db "created_at = this_week()")]
         (is (every? week ["today 00:00" "today 12:00" "today 23:59"]))
         (is (not (week "a week ago")))))

     (testing "this_month(): today is in it, 31 days ago never is"
       (let [month (names db "created_at = this_month()")]
         (is (every? month ["today 00:00" "today 12:00" "today 23:59"]))
         (is (not (month "31 days ago"))))))))
