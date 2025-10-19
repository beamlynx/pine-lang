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