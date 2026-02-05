# Change Log
All notable changes to this project will be documented in this file. This change
log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [Unreleased]

## [0.30.0] - 2026-02-04
### Added
- Support for heuristic relations based on column naming conventions. This is helpful when foreign keys are not explicitly specified.


## [0.29.0] - 2025-12-25
### Added
- Support for cursor position aware hints. This is helpful when the user isn't at the end of the expression. Hints are generated based on the cursor position. The build endpoint supports a new parameter `cursor` which must contain the `line` and `character` position of the cursor.

## [0.28.0] - 2025-12-08
### Added
- Support for date extraction functions in the select operation e.g.
```
employee | select: created_at => year
employee | select: created_at => year as created_at_year
```

Supported functions are: `year`, `month`, `week`, `day`, `hour`, `minute`

- Group on derived columns e.g.
```
employee | select: created_at => month | group: month => count
```

- Explicit join columns are supported in the join operation e.g.
```
company | employee .company_id = .id
```

### Removed
- Internal state field `:join-map` has been removed. This was legacy dead code kept since v0.8.0 that was never actually used. The `:joins` vector format continues to be used for SQL generation.

## [0.27.0] - 2025-10-19
### Added
- Column aliases are supported in the order operation e.g.
```
company as c | o: c.name asc
```

- Support comments in the expressions e.g.
```
company | -- This is a line comment
company | /* This is a multi-line block comment */
```

## [0.26.1] - 2025-09-07
### Changed
- Using a readonly db user for the playground

## [0.26.0] - 2025-09-06
### Added
- Support for raw SQL queries:
```
POST /api/v1/sql
```

## [0.25.0] - 2025-08-28

### Added
- Values are type casted to the appropriate database column type.

### Fixed
- As we use the correct typecase for the values, it is possible to update a jsonb column.

- It is possible to use a LIKE operator on a uuid using a type cast e.g.
```
company | where: id like '9cd%' ::uuid
```

## [0.24.0] - 2025-08-25
### Added
- Support for `update!` operation:
```
customers | w: id = 1 | update! name = 'John Doe'
```
- Return id columns for all tables in the result. This allows in-place updates on a query result.

### Fixed
- All columns were being returned in some cases instead of the explicitly selected columns. e.g.
```
company | s: id,
company | s: id | l: 1
```

## [0.23.0] - 2025-08-15
### Added
- Setup for playground

### Fixed
- Numbers are parsed as longs e.g. if the column is an integer:
```
customers | id = 1
```

## [0.22.0] - 2025-07-12
### Added
- Column hints for `where:` operation, supporting partial expressions:
```
company | where:           # Shows all columns
company | w: i             # Shows columns matching 'i' (like 'id')  
company | w: id =          # Shows all columns after specifying column + operator
y.employee | w: comp       # Shows columns matching 'comp' (like 'company_id')
```

## [0.21.0] - 2025-07-02
### Fixed
- Docker image wasn't running.Updated the base image to `openjdk:11-jre-slim`

### Added
- Support for `ilike`, `not like`, and `not ilike` operators:
```
company | where: name ilike 'acme%'
company | where: name not like 'test%'
company | where: name not ilike 'admin%'
```
- Support for casting columns as `::uuid`
- Support for dates in conditions e.g.
```
company | where: created_at > '2025-01-01' | created_at < '2026-01-01'
```

## [0.20.0] - 2025-06-22
### Added

- Specify join types i.e. `LEFT JOIN` or `RIGHT JOIN`:
```
x | y :left
x | y :right
```

### Breaking
- Syntax for specifying parent and child relations is changed (introduced in `0.6.0`). This avoids the need for backtracking.
```
x | of: y
x | has: y
```
is now:
```
x | y :parent
x | y :child
```

- `^` is removed from the syntax to specific the directionality of the join. (introduced in `0.6.0`)

## [0.19.0] - 2025-06-21
### Added
- Support for casting columns in conditions e.g.
```
company | where: id like '9cd%' ::text
```

## [0.18.0] - 2025-06-04
### Added
- Support for `group` operation:
```
email | group: status => count
```

- Column aliases are supported in conditions e.g.
```
tenant as t | company | where: t.id = 'xxx'
```

### Changed
- Default limit is removed for `count:` and `delete:` operations.
- For `count:` operations, the `with` SQL clause is used to build the nested query e.g.

```pine
company | count:
```

is evaluated to:

```sql
WITH x AS (SELECT * FROM "public"."company") SELECT COUNT(*) FROM x;
```

## [0.17.0] - 2025-05-03
### Fixed
- Using database connection pooling
- Using UTC dates

## [0.16.0] - 2025-02-09
### Added
- Support for connection stats which contain the number of db connections.
```
GET /connection/stats

{
  "connection-count": 10,
  "time": "2025-02-10T01:49:53.808120858"
}
```



## [0.15.0] - 2025-02-02
### Added
- Column hints when using the order operation:

```
company | o:
company | o: id,
```

### Fixed
- Columns hints for the correct table are show e.g. the following was showing hints for `company` to begin with:
```
company | s: id | document | s:
```


## [0.14.1] - 2025-01-09
### Fixed
- By default all columns are selected. When columns are specified, all columns are not returned e.g. this didn't work:

```
employee as e | document as d | s: e.id
```

## [0.14.0] - 2025-01-07

### Added
- Support for columns e.g. hints are generated for a partial select:
```
company | s:
company | s: id,
```

### Changed
- Connection id format is `host`:`port` instead of just the `host`.

## [0.13.0] - 2024-10-25
### Added
- DB connection management i.e. create a new connection and connect to it
```
POST /connections
POST /connections/:id/connect
```

- Support for booleans:
```
company | is_public = true
```

## [0.12.0] - 2024-10-18
### Added
- Support for `not in` operator
- Support for no operations e.g. `delete:`. Such operations are evaluated client side.

## [0.11.0] - 2024-09-12
### Added

- `where:` supports comparing values between columns of different tables

```
folder as f | document | where: name = f.name
folder as f | document | name != f.name
```


## [0.10.0] - 2024-09-12
### Added

- Support for `count:`:

```
company | count:
```

## [0.9.0] - 2024-09-04
### Added

- Support for `NULL`:

```
company | name is null
company | name is not null
```

which also works with the `=` operator:
```
company | name = null
company | name != null
```

- Support for `order`:
```
company | order: created_at
company | order: country, created_at asc
```

## [0.8.1] - 2024-07-30
### Fixed

- Specifying the join column in case of ambigious relations wasn't working.

## [0.8.0] - 2024-07-30
### Added
- Change the context using the `from:` keyword. This is helpful when the tables relations are not linear and look like a tree.
```
company as c | document | from: c | employee
```

### Breaking
- State: `joins` is a vector e.g. `[ "x" "y" ["x" "id" :has "y" "x_id"]]`

### Changed
- State: `join-map` is kept for legacy reasons but it is only used internally.

## [0.7.2] - 2024-07-26
### Changed
- No difference in functionality. Removed a lot of deprecated code - only keeping the code for reborn.

## [0.7.1] - 2024-07-26
### Fixed
- Allow spaces in the start of a pine expression

## [0.7.0] - 2024-07-26
### Added
- Support for `in` operator

### Changed
- Error type is returned. It is either nothing or `parse`.

## [0.6.0] - 2024-07-22
### Added
- Support for directional joins:
```
employee | has: employee
employee | of: employee
employee | employee^
```
- Columns can be qualified by table aliases:
```
employee as e | s: e.name
```

## [0.5.4] - 2024-07-16
### Fixed
- Incorrect hints were generated in case of ambiguity

## [0.5.3] - 2024-07-16
### Fixed
- Incorrect schema being returned in hints when joining from child to parent

## [0.5.2] - 2024-07-14
### Changed
- Default `limit` is `250` if not specified

### Fixed
- All columns weren't being select in some cases e.g. using `company | s: id | employee`, the columns from `employee` table weren't being selected

## [0.5.1] - 2024-07-11
### Added
- Context sensitive columns selection e.g. `company | s: id | employee | s: id`

## [0.5.0] - 2024-07-10

### Added
- Hints can be provided to resolve ambigious joins e.g. instead of `company | employee`, you can explicitly specify the join column i.e. `column | employee .company_id`
- The delete operation uses a nested query. The column used for deletes must be specified:

```pine
public.company | delete! .id
```

evaluates to:

```sql
DELETE FROM
  "public"."company"
WHERE
  "id" IN (
    SELECT
      "c_0"."id"
    FROM
      "public"."company" AS "c_0"
  );
```
- Conditions can be composed. Following are allowed:

```pine
company | where: id='xxx'
company | w: id='xxx'
company | id='xxx'
```

### Changed
- Conditions can't be combined with the tables e.g. `company id='xxx'`. Instead compose them using pipes: `company | id='xxx'`
- Double quotes around strings aren't supported anymore. Use single quotes i.e. instead of `id="xxx"`, use `id='xxx'`

### Removed
- Support for `group`, `order`, `set!` is dropped. It will be added soon in the up coming versions.
- Context sensitive columns selection

## [0.4.8] - 2024-06-24
### Fixed
- Strings can contain a `+` character.

## [0.4.7] - 2024-06-13
### Fixed
- Db host can be configrued using an environment variable: `DB_HOST`

## [0.4.6] - 2024-06-13
### Changed
- The host is returned as the connection id instead of an internal identifier.

## [0.4.5] - 2024-06-13
### Changed
- Updated configuration to require environment variables: `DB_NAME`, `DB_USER`, `DB_PASSWORD`


## [0.4.4] - 2024-06-13
### Fixed
- Support for multiple architectures i.e. amd64 and arm64

## [0.4.3] - 2024-06-11
### Fixed
- It wasn't possible to get the relations between tables using a readonly user.
- Generating an uberjar so that dependencies are not loaded when the server starts.

## [0.4.2] - 2024-05-04
### Added
- The context contains the schema as well.

### Fixed
- The values for the filters weren't being quoted properly in some cases

### Breaking
- The hints for tables contain an object of schema and table instead of just a string i.e. table.

## [0.4.1] - 2023-08-11
### Added
- Better hints i.e. taking into consideration the context e.g. for expression `document | ..`, only tables related to `document` will be suggested. Also only schemas of the related tables will be suggested.

### Changed
- Reverted the change for getting all the columns. Instead of listing all the columns, we are relying on the `*` again. The change was a remnant of bug related to the ordering of the columns which had to do nothing with explicitly specifying the columns.
- The `connection` protocol doesn't expose the `get-schema` method.

### Fixed
- Numbers as parameters wasn't working e.g. `file version>1`

### Breaking
- Dropped support for MySQL.
- All endpoints are prefixed with `/api/v1`


## [0.4.0] - 2023-07-28
### Added
- Disabled CORS
- API endpoint for getting the active connection:
```
GET /connection

{
  ...
  "connection-id": "..."
}

```
- When using `POST /build` the response also includes the `connection-id`, and `params`
- When using `POST /eval` the response also includes the `connection-id`, `query`, and `params`.
- In case of an error, it is handled and the error message is returned in the API response as `error`
- Limited support for showing hints based on the input

### Fixed
- Pine expression build/eval was failing if the db connection isn't initialized
- An error was being thrown when using `uuid` values in the expressions: `operator does not exist: uuid = character varying`
- Order of the columns in the result was sometimes not the same as the order in the query. Also, all columns are explicitly selected in the sql instead of relying on `*`

## [0.3.1] - 2022-02-14
### Added
- API endpoint for building expressions:
```
POST /build
{
  "expression": "user"
}


{
  ...
  "query": "\nSELECT user_0.* FROM \"user\" AS user_0 WHERE true;\n"
}
```

- API endpoint for evaluating expressions:
```
POST /eval
{
  "expression": "user"
}

{
  ...
  "result": [
    {
      "email": "john@acme.com",
      "name": "John Doe",
      ...
    },
    ...
  ]
}
```
- API endpoint for setting the connection:
```
PUT /connection
{
  "connection-id": "default"
}

{
  "connection-id": "default"
}
```
- API endpoint for getting the connections
```
GET /connections

[
  "result": [ "default", "mysql-test" ]
]
```

### Deprecated
- API endpoint for building sql expressions `POST /pine/build`. Use the new endpoint: `POST /build`.

## [0.3.0] - 2022-02-10
### Added
- Support for Postgres

### Breaking changes
- Default limit of `50` is removed for updates and `1` for deletes
- Unselecting of columns is disabled. This will be enabled again in a future release.
```
customers | unselect: id
```
- Format of the config file is changed. This was done to support multiple
  connection configurations. The `:connection-id` property can be set to select
  the default connection.

## [0.2.0] - 2019-04-26
### Added
- Unselecting of columns
```
customers | unselect: id
```

### Fixed
- It wasn''t working:
```
customers industry=""
```
- Setting string values wasn't working e.g.
```
customers 1 | set! industry="Test"
customers 1 | set! industry=123
```

## 0.1.0 - 2019-04-21
### Added
- Check out the [features][features] document for a list of features

[Unreleased]: https://github.com/ahmadnazir/pine/compare/0.3.1...HEAD
[0.3.1]: https://github.com/ahmadnazir/pine/compare/0.3.0...0.3.1
[0.3.0]: https://github.com/ahmadnazir/pine/compare/0.2.0...0.3.0
[0.2.0]: https://github.com/ahmadnazir/pine/compare/0.1.0...0.2.0
[features]: FEATURES.md
