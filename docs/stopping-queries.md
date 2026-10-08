# Stopping a query

A client can stop a query that is still running. The database stops the statement too, so it no
longer holds a connection or locks.

## How

1. Make up an id for the run, such as a UUID, and send it as `run-id` with `/api/v1/eval` or
   `/api/v1/sql`.
2. To stop it, send the same id to `/api/v1/cancel`:

   ```json
   POST /api/v1/cancel
   { "run-id": "5f0c…" }
   ```

The stopped request answers with:

```json
{ "error-type": "cancelled", "error": "Query stopped." }
```

`/cancel` answers `{ "running": true }` when it stopped a statement, and `{ "running": false }`
otherwise. It is not an error to stop a run that has finished or hasn't started yet.

## What to know

- **A write is rolled back.** Every `delete!` and `update!` runs in a transaction, so a stopped write
  changes nothing, including an `update!` across several tables.
- **A stop can arrive first.** If `/cancel` arrives before the run's statement starts, the statement is
  refused when it does.
- **Use a new id for every run.** A stop for a run that has already finished is kept for 10 minutes, in
  case the run hasn't arrived yet. A new run that reuses the id in that time is stopped at once.
- **The 60-second limit is separate.** A statement that runs past it fails with the database's own
  error, not `cancelled`.
- A request without `run-id` can't be stopped. It behaves as before.
