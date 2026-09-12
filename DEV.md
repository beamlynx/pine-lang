# How to run tests while using the repl?

- `pine.db.main` -> `connection-id` needs to be reset.
- `pine.db.main` -> `references` needs to be reset.

# Version Synchronization

The version in `src/pine/version.clj` must match the Docker image version in `playground.docker-compose.yml`. This is enforced by:

- **Pre-commit hook**: Automatically checks version sync before each commit
- **CI check**: GitHub Actions will fail if versions don't match

To manually check version sync:
```bash
./scripts/check-version-sync.sh
```

If versions are out of sync, update one of the files to match the other.

# Connecting to the playground db

Run the service:

```bash
docker-compose -f playground.docker-compose.yml up sample-db-ecommerce
```

From the UI, connect to the database using port 5434 (See: `playground.docker-compose.yml`).

# Local Postgres + MySQL sandbox (manual testing, both dialects)

`dev.docker-compose.yml` brings up a read-write Postgres and a read-write
MySQL side by side, seeded with the same tables and the same data
(`docker/db/init` and `docker/mysql/init` respectively -- the MySQL seed is
a hand-translation of the Postgres one, kept in sync by hand). Unlike the
playground above, both are read-write, so `update!`/`delete!` are testable
too, and there's no bundled `pine` server image -- whatever dialect work
you're testing here is likely still unreleased, so run pine-lang from
source against it instead:

```bash
docker compose -f dev.docker-compose.yml up -d
clj -M:run-dev   # or clj -M:run
```

Connection details:

| | Postgres | MySQL |
|---|---|---|
| host | localhost | localhost |
| port | 5435 | 3308 |
| database | pine | pine |
| user / password | pine / pine | pine / pine |

```bash
curl -X POST http://localhost:33333/api/v1/connections \
  -H "Content-Type: application/json" \
  -d '{"dbtype":"mysql","host":"localhost","port":3308,"dbname":"pine","user":"pine","password":"pine"}'
```

The MySQL seed additionally has `warehouses`/`warehouse_staff` (a
heuristic-only relation, no real FK -- see `references.clj`'s naming-based
detection) and a second database, `pine_other`, with `SELECT` granted to
the `pine` user -- a live check that MySQL's `DATABASE()`-scoped schema
introspection actually excludes it (unlike Postgres, MySQL's
`information_schema` is server-global, not connection-scoped).

Kept out of `playground.docker-compose.yml` on purpose (different ports,
read-write instead of read-only, no `pine` image) so
`check-version-sync.sh` -- which only greps `playground.docker-compose.yml`
-- never sees it.