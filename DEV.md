# How to run tests while using the repl?

- `pine.db.main` -> `connection-id` needs to be reset.
- `pine.db.main` -> `references` needs to be reset.

# Connecting to the playground db

Run the service:

```bash
docker-compose -f playground.docker-compose.yml up sample-db-ecommerce
```

From the UI, connect to the database using port 5434 (See: `playground.docker-compose.yml`).