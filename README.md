# Elekto.Mcp.Sql

[![.NET](https://img.shields.io/badge/.NET-10-512BD4)](https://dotnet.microsoft.com)
[![NuGet](https://img.shields.io/nuget/v/Elekto.Mcp.Sql.svg)](https://www.nuget.org/packages/Elekto.Mcp.Sql)
[![NuGet Downloads](https://img.shields.io/nuget/dt/Elekto.Mcp.Sql.svg)](https://www.nuget.org/packages/Elekto.Mcp.Sql)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![CI](https://github.com/elekto-com-br/elekto-mcp-sql/actions/workflows/ci.yml/badge.svg)](https://github.com/elekto-com-br/elekto-mcp-sql/actions/workflows/ci.yml)

Read-only MCP server for SQL Server 2017+ introspection and querying (tested on 2019 and 2022).
Exposes schema metadata, object definitions, and data queries via the MCP protocol (stdio),
allowing GitHub Copilot (and other MCP clients, like Claude, etc.) to understand your database structure
without storing credentials in the repository.

## ⚠️ Privacy and Data Security Warning

MCP servers act as a bridge between your local data and AI language models. When you use
this server with an AI assistant (such as GitHub Copilot, Claude, or others), the following
happens:

1. The AI agent calls tools on this server to read data from your SQL Server database.
2. The results — which may include table schemas, stored procedure definitions, or actual
   row data — are sent back to the AI agent and transmitted to the LLM provider's
   infrastructure for analysis.
3. **This means your data leaves your machine and is sent to a third-party service**
   (Microsoft, Anthropic, OpenAI, etc.), subject to their respective terms of service
   and privacy policies.

Before connecting this server to any database, carefully consider:

- What data could be read? Does it include PII, financial records, trade secrets,
  or other sensitive information?
- Who is the LLM provider and what are their data retention and privacy policies?
- Are you authorized to share this data with that third party under applicable laws
  and regulations?

**Recommendations:**

- Never connect to databases containing sensitive data unless you have explicitly assessed
  and accepted this risk.
- Use database accounts with the minimum required privileges (read-only, restricted to
  specific schemas where possible).
- Use `max_query_rows` to limit how much data can be returned in a single call.
- Prefer databases with anonymized or synthetic data for development and exploration.
- AI agents can be **extremely creative** in finding ways to execute a task. Altouht this server 
  is designed to be read-only and to validate all inputs, **there is always a risk of 
  unintended consequences** when exposing database access to an AI agent.

**Regardless of the precautions you take, the responsibility for any consequences arising
from the use of this tool rests entirely with you.** This software is provided *as is*
with no warranties of any kind.

## Available Tools

| Tool                       | Description                                                  |
| -------------------------- | ------------------------------------------------------------ |
| `list_databases`           | Databases registered in the configuration                    |
| `get_database_overview`    | High-level database summary (counts, size, connection metadata), and whether the login sees everything |
| `check_permissions`        | What the connected login can and cannot see, the GRANT that completes it, and whether it could write |
| `get_schema_summary`       | Aggregated metrics by schema (objects, rows, size)           |
| `list_schemas`             | Schemas in a database (excluding system schemas)             |
| `list_tables`              | User tables with schema, dates, approximate rows and estimated size; filterable by schema and name pattern |
| `list_views`               | User views, filterable by schema and name pattern            |
| `find_columns`             | Every table and view holding a column whose name matches a pattern |
| `list_procedures`          | User stored procedures (with basic complexity metrics)       |
| `list_functions`           | User-defined functions (with basic complexity metrics)       |
| `get_table_schema`         | Columns with unambiguous type declarations, all extended properties, PKs, FKs, checks, uniques and indexes with key order and declared key width |
| `get_view_definition`      | DDL definition + columns of a view, with the same column detail |
| `get_procedure_definition` | CREATE PROCEDURE text                                        |
| `get_function_definition`  | CREATE FUNCTION text                                         |
| `get_dependency_graph`     | Object dependency edges (FK + SQL dependencies)              |
| `get_table_usage`          | References to a table across FKs and SQL modules             |
| `get_data_profile`         | Column profile (null ratio, distinct count, min/max, top values) |
| `get_index_health`         | Duplicate/unused index diagnostics + missing-index suggestions |
| `compare_schemas`          | Compares table/column structure between two configured databases |
| `generate_dependency_dot`  | Graphviz DOT dependency graph with node metadata (`node_kind`) |
| `query_table`              | SELECT from a table or view with filtering, grouping, secure aggregates, sorting, sampling and pagination |

## Upgrading from 1.x

Version 2.0.0 changes what the tools return and how their parameters are declared. Nothing
needs reconfiguring — connection files, `.mcp.json` and the CLI arguments are unchanged —
but anything that parses the output will notice:

| Change | What to do |
| ------ | ---------- |
| `query_table` returns an object, not an array | Read the rows from `rows`; check `truncated` |
| Failures return `ok: false` content instead of raising a tool error | Test for `ok === false` before treating the payload as data |
| Column results gained `type_declaration`, `max_length_chars`, `is_persisted` and `extended_properties` | Prefer `type_declaration` over `max_length`, which is bytes |
| `description` on a column is now an alias for `extended_properties.MS_Description` | Nothing; it still works |
| Optional tool parameters are no longer nullable | Nothing over MCP. Direct C# callers pass `""` (or `0`) instead of `null` |
| New: `find_columns`; `list_tables` and `list_views` take a `name_pattern` | Nothing; both are additive |

Everything else — every other tool, every other field — is unchanged.

## What changed in 2.1.0

Nothing changes shape: arrays are still arrays and objects keep every field they had. A few
values now say "unknown" instead of passing a guess for a fact, which is worth knowing if you
parse them:

| Change | Why |
| ------ | --- |
| New: `check_permissions` | Says what the login is missing, per tool, with the GRANT that fixes it |
| `get_database_overview`, `get_table_usage`, `get_index_health` and `generate_dependency_dot` gained a `visibility` block | So a result the login could only partly see says so |
| `list_procedures` / `list_functions` rows gained `definition_visible`; `line_count` and `join_count` are `null` when it is false | A hidden definition used to read as a body of 0 lines |
| `referenced_object_count` is `null`, and `get_table_usage`'s `sql_module_usage` is `null`, when the login cannot read `sys.sql_expression_dependencies` | Both used to fail the whole call with error 229 |
| `get_index_health` returns the duplicates and `null` for the DMV sections when the login lacks the server permission | It used to fail outright, losing the part that needs no permission |
| `get_*_definition` of a name the login cannot see returns `ok: false` instead of `[]` | `[]` read the same for "does not exist" and "hidden from you" |
| SQL errors carry their number, and the hint tells permission, missing name and syntax apart | One generic hint covered all three |

## Reading the Results

Three things about the shape of what comes back are worth knowing before you rely on it.

### Column types are reported twice, on purpose

`sys.columns.max_length` is documented in **bytes**. A `nvarchar(250)` column therefore
reports `500`, and reading that as characters is wrong by a factor of two — silently, because
nothing downstream contradicts it. That raw value is still reported, for fidelity to the
catalog, but never on its own:

```json
{
  "column_name": "Tag1",
  "data_type": "nvarchar",
  "type_declaration": "nvarchar(250)",
  "max_length": 500,
  "max_length_chars": 250
}
```

Use `type_declaration` or `max_length_chars`. They cannot be misread.

Columns also carry `extended_properties` — every property, not only `MS_Description` —
so an application's own conventions (display formats, units, masks) are visible, and
`is_persisted` for computed columns.

### `query_table` returns an envelope, not a bare array

A bare array of two rows cannot be told apart from a table that holds two rows. The
result therefore states what it is:

```json
{
  "table": { "schema": "Feeder", "name": "GenericSecurity" },
  "row_count": 100,
  "truncated": true,
  "top_applied": 100,
  "skip": 0,
  "max_query_rows": 10000,
  "rows": [ ... ]
}
```

`truncated` is measured rather than inferred: one row past the limit is fetched and
discarded. `top_requested` appears only when `max_query_rows` overrode what was asked for.

### Failures come back as content

An exception thrown from an MCP tool does not reach the caller — the host replaces it with
a generic line. Failures are therefore returned as a normal result carrying `ok: false`:

```json
{
  "ok": false,
  "tool": "query_table",
  "error": "'columns' looks like JSON: [\"Source\", \"Name\"]",
  "hint": "'columns' is a plain comma-separated string, not a JSON array or object.",
  "example": { "columns": "Source, Name, ReferenceDate" }
}
```

The trade-off is deliberate: the host no longer marks the call as an error, but the caller
can read what went wrong and correct it. Successful results are unchanged and never carry
an `ok` field.

### What the login cannot see is left out without an error

SQL Server shows a login only the objects it holds some permission on (*metadata visibility*),
and it filters the rest out silently. A login in `db_datareader` alone sees every table but
not one procedure it cannot execute, and sees views without their text. Every catalog query
then returns a normal, well-formed, **short** answer — "this database has 6 procedures" when
it has 672.

Since the data cannot reveal that, the server checks the permissions that decide it and says
so. `get_database_overview`, the tool to call first, carries:

```json
"visibility": {
  "complete": false,
  "notes": [
    "The login lacks VIEW DEFINITION on the database. SQL Server then leaves out, without any error, every procedure, function and view the login holds no permission on, so their listings and counts may be short.",
    "756 of the 756 visible views, procedures and functions have their definition hidden from this login."
  ],
  "hint": "Call check_permissions for what this login is missing and the GRANT statements that fix it."
}
```

`check_permissions` then reports, for each group of tools, `complete`, `partial` or
`unavailable`, the permission that is missing and the GRANT that adds it, in the syntax of the
server's version. It also reports `write_access`: whether a role, a database permission or a
schema- or object-level GRANT lets the login change anything, so that an account meant to be
read-only can be checked rather than assumed.

## Installation

### As a .NET global tool (recommended)

Requires [.NET 10 Runtime](https://dotnet.microsoft.com/download/dotnet/10.0) or SDK.

```powershell
dotnet tool install -g Elekto.Mcp.Sql
```

Upgrade to a newer version:

```powershell
dotnet tool update -g Elekto.Mcp.Sql
```

After installation the `elekto-mcp-sql` command is available on PATH.
Use it directly in `.mcp.json` — no path needed:

```json
{
  "servers": {
    "sql": {
      "type": "stdio",
      "command": "elekto-mcp-sql"
    }
  }
}
```

> **Zero-config:** if your project already has a `ConnectionStrings` section in
> `appsettings.json`, `web.config` or `App.config`, the server picks it up automatically
> and no further configuration is required.

### From a local publish (air-gapped / corporate environments)

```powershell
cd src
dotnet publish -c Release -o C:\Tools\Elekto.Mcp.Sql
```

```json
{
  "servers": {
    "sql": {
      "type": "stdio",
      "command": "dotnet",
      "args": ["C:\\Tools\\Elekto.Mcp.Sql\\Elekto.Mcp.Sql.dll"]
    }
  }
}
```

## Configuration

`--connections <path>`, when given, is used on its own and no other source is consulted.

Otherwise every source below is read and **merged**, so a database defined in one source and
a database defined in another are both available. Where the same database name appears in
more than one, the higher-priority source wins:

| Priority       | Source                                                                          |
| -------------- | ------------------------------------------------------------------------------- |
| 1 (highest)    | `.elekto.mcp.sql.local.json` in the working directory (project root)             |
| 2              | `ConnectionStrings` in `appsettings.Development.json`                            |
| 3              | `ConnectionStrings` in `appsettings.json`                                        |
| 4              | `<connectionStrings>` in `App.config` / `web.config`                             |
| 5              | `.elekto.mcp.sql.local.json` in the user's home directory (`~`)                  |
| 6 (lowest)     | `MCP_SQL_CONNECTIONS` environment variable (legacy compatibility)                |

Note that the home-directory file sits *below* the project's `appsettings.json`: it holds
your defaults, and the project it is used in overrides them.

At startup the server logs every source that contributed to stderr, making it easy to
diagnose which files are in effect.

### Zero-config for existing .NET projects

If your project already has `appsettings.json` or `web.config` with a `ConnectionStrings`
section, the server will pick them up automatically — no extra file needed.

**Be Careful**: the automatic discovery is convenient but may use a project connection too powerful for safe use with AI agents. 
If your existing connection strings have write permissions or access to sensitive data, consider using a separate connections file 
with read-only credentials and specifying it explicitly via `--connections` or by placing it in the project root.

### Connection file format

The file is a JSON object mapping logical database names to their configurations.

**Simple format** (direct connection string):

```json
{
  "MyDatabase": "Server=SQLSRV01\\INST;Database=MyDatabase;Integrated Security=SSPI"
}
```

**Full format** (with options):

```json
{
  "MyDatabase": {
    "connection_string": "Server=SQLSRV01\\INST;Database=MyDatabase;Integrated Security=SSPI",
    "max_query_rows": 5000,
    "default_timeout_seconds": 30
  }
}
```

Both formats can be mixed in the same file. See [`sample-connections.json`](sample-connections.json)
for a ready-to-use example.

The recommended location for the local file is the project root (auto-discovered) or `~`
(shared across all projects). Both paths are already in `.gitignore`.

### Options per database

| Option                    | Type    | Default  | Description                          |
| ------------------------- | ------- | -------- | ------------------------------------ |
| `connection_string`       | string  | required | SQL Server connection string         |
| `max_query_rows`          | integer | 10 000   | Maximum rows returned per query call |
| `default_timeout_seconds` | integer | 30       | SQL command timeout in seconds       |

### Environment variable expansion in connection strings

Use `%{VARIABLE_NAME}` inside connection strings to avoid storing credentials in plain text.
Variables are resolved from the process environment at server startup.

```json
{
  "CRM": {
    "connection_string": "Server=SQLSRV01;Database=CRM;User Id=%{CRM_DB_USER};Password=%{CRM_DB_PASS}",
    "max_query_rows": 2000
  }
}
```

`%{CRM_DB_USER}` and `%{CRM_DB_PASS}` are replaced by the values of the corresponding
OS environment variables. If a referenced variable does not exist, the server fails with
an explicit error message.

### Fallback: MCP_SQL_CONNECTIONS environment variable

If `--connections` is not supplied, the server falls back to reading the
`MCP_SQL_CONNECTIONS` environment variable, which must contain the JSON directly.
This is provided for backward compatibility; the file-based approach is recommended.

## Visual Studio 2026 Setup (.mcp.json)

Create or edit `.mcp.json` at the solution root (or in your user profile for global use).

### Recommended: local connections file (zero-config)

Drop a `.elekto.mcp.sql.local.json` file in the project root or in `~`; the server
finds it automatically. No arguments needed in `.mcp.json`:

```json
{
  "servers": {
    "sql": {
      "type": "stdio",
      "command": "dotnet",
      "args": ["D:\\Tools\\Elekto.Mcp.Sql\\Elekto.Mcp.Sql.dll"]
    }
  }
}
```

### Alternative: explicit path via --connections

Point the server to any file via `--connections`. Useful when the file lives outside the
project tree or when you need to switch between profiles:

```json
{
  "servers": {
    "sql": {
      "type": "stdio",
      "command": "dotnet",
      "args": [
        "D:\\Tools\\Elekto.Mcp.Sql\\Elekto.Mcp.Sql.dll",
        "--connections",
        "C:\\Users\\YourName\\sql-connections.json"
      ]
    }
  }
}
```

The connection file itself stays outside the repository, so credentials are never
committed to source control.

### Alternative: environment variable (legacy)

If you prefer not to use a file, you can still pass the JSON via an environment variable.
Note that backslashes require double escaping inside JSON-within-JSON (`\\\\`):

```json
{
  "servers": {
    "sql": {
      "type": "stdio",
      "command": "dotnet",
      "args": ["D:\\Tools\\Elekto.Mcp.Sql\\Elekto.Mcp.Sql.dll"],
      "env": {
        "MCP_SQL_CONNECTIONS": "{\"MyDb\": {\"connection_string\": \"Server=SQLSRV01\\\\INST;Database=MyDb;Integrated Security=SSPI\"}}"
      }
    }
  }
}
```

After saving `.mcp.json`, Copilot automatically restarts the server.
Tools are disabled by default: enable them in the Copilot Chat tools panel.

## Build and Publish

```powershell
cd Elekto.Mcp.Sql\src
dotnet publish -c Release -o C:\Tools\Elekto.Mcp.Sql
```

Requires .NET 10 installed on the machine. The published directory is ~7 MB (NuGet dependencies).
For internal use, this is preferred over self-contained (~81 MB).

### Running the tests

From the repository root:

```bash
dotnet test
```

The test project holds two kinds of tests:

- **Unit tests** (such as `ConnectionConfigTests`) need nothing beyond the .NET SDK.
- **Integration tests** (`SchemaReaderTests`) run against a real SQL Server. Each run creates a
  database named `ElektoMcpTest`, fills it with a small seed schema, and drops it at the end.

The integration tests look for a SQL Server in this order and use the first one found:

1. **The `ELEKTO_MCP_SQL_CONN_TEST` environment variable.** If set, it must hold a connection
   string to a server the tests may use. The database named in it, if any, is ignored. The login
   needs permission to create and drop databases. If this server cannot be reached, the tests fail
   right away instead of trying the next options, since an explicit choice that does not work is
   an error worth seeing.
2. **LocalDB**, only on Windows and only if the `(localdb)\MSSQLLocalDB` instance answers.
3. **A disposable container** started through [Testcontainers](https://dotnet.testcontainers.org/)
   from the `mcr.microsoft.com/mssql/server:2022-latest` image. This needs Docker (rootless Docker
   works too). The container is started only when an integration test needs it, and it is removed
   when the run ends. The first run also downloads the image, which takes a while.

If none of these is available, the integration tests fail with a message listing what was tried
and why each option did not work. The test output states which server was used.

Some of the integration tests run as restricted logins, to check what the tools say when SQL Server
hides objects. The fixture creates those logins (`ElektoMcpTest_*`) and drops them at the end, which
needs `ALTER ANY LOGIN` as well; a server that does not allow it reports those tests as skipped,
with the reason.

Example, pointing the tests at an existing server:

```bash
export ELEKTO_MCP_SQL_CONN_TEST="Server=localhost,1433;User Id=sa;Password=<password>;TrustServerCertificate=True"
dotnet test
```

Do not point `ELEKTO_MCP_SQL_CONN_TEST` at a server where a database called `ElektoMcpTest`
matters to anyone: the tests drop and recreate it.

To run only the unit tests, with no SQL Server at all:

```bash
dotnet test --filter "FullyQualifiedName!~SchemaReaderTests"
```

## Recommended permissions

Everything the tools read, with no permission to change anything:

```sql
USE [YourDatabase];
CREATE USER [mcp_reader] FOR LOGIN [mcp_reader];  -- if the user does not exist yet
ALTER ROLE db_datareader ADD MEMBER [mcp_reader];  -- tables and views; also covers sys.sql_expression_dependencies
GRANT VIEW DEFINITION TO [mcp_reader];             -- procedures, functions, view text, complete dependencies

USE master;
GRANT VIEW SERVER STATE TO [mcp_reader];           -- get_index_health: unused and missing indexes
```

On SQL Server 2022 and later the last grant can be narrowed to the permission the index DMVs
actually need:

```sql
USE master;
GRANT VIEW SERVER PERFORMANCE STATE TO [mcp_reader];
```

| Permission | Without it |
| ---------- | ---------- |
| `db_datareader` (SELECT on the database) | Tables and views the login holds no grant on are left out; `query_table` fails on them. If the login is not in `db_datareader`, `sys.sql_expression_dependencies` also needs `GRANT SELECT ON sys.sql_expression_dependencies` |
| `VIEW DEFINITION` on the database | Procedures and functions are left out, view and module text is hidden, dependencies are incomplete |
| `VIEW SERVER STATE` (2022+: `VIEW SERVER PERFORMANCE STATE`) | `get_index_health` returns the duplicate indexes only |

None of these lets the login change data or schema. Run `check_permissions` against the
database to see which are missing — it prints the statements for that login and that
server version.

## Limits and Security

- Read-only: only SELECT on tables and views. DML and procedure/function execution are not supported.
- `query_table` builds SQL internally from validated parameters. Identifiers (table, schema,
  columns) are validated against a regular expression before being composed into SQL.
- The WHERE clause is accepted as free text (necessary for flexibility), but DML is impossible
  since the command is always built as `SELECT TOP n ... FROM [t] WHERE ...`.
- `max_query_rows` caps the maximum number of rows returned per database (default 10,000).
  The `top` parameter in `query_table` is always clamped to this value, and the result says
  so via `top_requested` and `truncated` rather than quietly returning a short answer.
- **Even so**, avoid exposing this server in untrusted environments or with sensitive data.
  Use firewalls and access policies to restrict who can execute queries via MCP. Use
  database accounts with the minimum required privileges (read-only) for all configured
  connections.
