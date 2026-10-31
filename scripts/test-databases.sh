#!/usr/bin/env bash
# Runs every test, with the real-database ones on SQLite, MySQL and Postgres.
#
# Starts a throwaway Postgres and MySQL in Docker, runs `clojure -M:test`
# against them, and stops them again, whatever the result. Arguments go to
# the test runner: `scripts/test-databases.sh -n pine.db.writes-test`.
#
# The same versions run in CI (.github/workflows/test.yml).
set -euo pipefail

POSTGRES_IMAGE=postgres:15
MYSQL_IMAGE=mysql:8.4
POSTGRES_PORT=${PINE_TEST_POSTGRES_PORT:-55452}
MYSQL_PORT=${PINE_TEST_MYSQL_PORT:-55453}
POSTGRES_NAME=pine_test_postgres_$$
MYSQL_NAME=pine_test_mysql_$$

stop() { docker rm -f "$POSTGRES_NAME" "$MYSQL_NAME" >/dev/null 2>&1 || true; }
trap stop EXIT

docker run -d --rm --name "$POSTGRES_NAME" -e POSTGRES_USER=pine -e POSTGRES_PASSWORD=pine \
  -e POSTGRES_DB=pine -p "$POSTGRES_PORT":5432 "$POSTGRES_IMAGE" >/dev/null
docker run -d --rm --name "$MYSQL_NAME" -e MYSQL_ROOT_PASSWORD=pine \
  -p "$MYSQL_PORT":3306 "$MYSQL_IMAGE" >/dev/null

# Each image starts a temporary server while it sets up, then the real one,
# so readiness is checked over TCP, which only the real one listens on.
until docker exec "$POSTGRES_NAME" pg_isready -h 127.0.0.1 -U pine -q 2>/dev/null; do sleep 0.5; done
until docker exec "$MYSQL_NAME" mysqladmin ping -h 127.0.0.1 -uroot -ppine --silent >/dev/null 2>&1; do sleep 0.5; done

PINE_TEST_REQUIRE_SERVERS=1 \
PINE_TEST_POSTGRES_URL="postgres://pine:pine@localhost:$POSTGRES_PORT/pine" \
PINE_TEST_MYSQL_URL="mysql://root:pine@localhost:$MYSQL_PORT/mysql" \
  clojure -M:test "$@"
