# SQL hints

What beamlynx's SQL editor completes from, while SQL is typed.

## Tables and columns

```
POST /api/v1/sql-schema
{"connection-id": "..."}
```

```json
{"dialect": "postgres",
 "default-schema": "public",
 "tables": [{"schema": "public", "table": "company", "columns": ["id", "name"]}]}
```

`connection-id` is optional. Without it, the selected connection is used. With no connection, or one
whose schema can't be read, `tables` is empty. That isn't an error, because the editor asks as soon
as it opens.

- `dialect` is `postgres`, `mysql` or `sqlite`. The editor uses it to quote names: a name like
  `tenantId` needs quotes in Postgres.
- `default-schema` is the schema an unqualified table is in: `public` in Postgres, and the one schema
  of a MySQL or SQLite connection. The editor offers those tables without a schema.
- Columns are in the table's order.

The endpoint is internal to beamlynx. It isn't on the MCP surface.
