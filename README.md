# Elekto.Mcp.Sql

Read-only MCP server for SQL Server 2022+ introspection and querying.
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
| `get_database_overview`    | High-level database summary (counts, size, connection metadata) |
| `get_schema_summary`       | Aggregated metrics by schema (objects, rows, size)           |
| `list_schemas`             | Schemas in a database (excluding system schemas)             |
| `list_tables`              | User tables with schema, dates, approximate rows and estimated size |
| `list_views`               | User views                                                   |
| `list_procedures`          | User stored procedures (with basic complexity metrics)       |
| `list_functions`           | User-defined functions (with basic complexity metrics)       |
| `get_table_schema`         | Columns, PKs, FKs, checks, uniques, indexes and computed/collation metadata |
| `get_view_definition`      | DDL definition + columns of a view                           |
| `get_procedure_definition` | CREATE PROCEDURE text                                        |
| `get_function_definition`  | CREATE FUNCTION text                                         |
| `get_dependency_graph`     | Object dependency edges (FK + SQL dependencies)              |
| `get_table_usage`          | References to a table across FKs and SQL modules             |
| `get_data_profile`         | Column profile (null ratio, distinct count, min/max, top values) |
| `get_index_health`         | Duplicate/unused index diagnostics + missing-index suggestions |
| `compare_schemas`          | Compares table/column structure between two configured databases |
| `generate_dependency_dot`  | Graphviz DOT dependency graph with node metadata (`node_kind`) |
| `query_table`              | SELECT from a table or view with filtering, grouping, secure aggregates, sorting, sampling and pagination |

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

The server resolves connections by walking the following chain and using the first match:

| Priority | Source                                                       |
| -------- | ------------------------------------------------------------ |
| 1        | `--connections <path>` argument                              |
| 2        | `.elekto.mcp.sql.local.json` no diretório de trabalho (raiz do projeto) |
| 3        | `.elekto.mcp.sql.local.json` no diretório home do usuário (`~`) |
| 4        | Seção `ConnectionStrings` em `appsettings.json` / `appsettings.Development.json` |
| 5        | Elemento `<connectionStrings>` em `web.config` / `App.config` |
| 6        | Variável de ambiente `MCP_SQL_CONNECTIONS` (compatibilidade legada) |

At startup the server logs the source it chose to stderr, making it easy to diagnose
which file is in effect.

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

## Limits and Security

- Read-only: only SELECT on tables and views. DML and procedure/function execution are not supported.
- `query_table` builds SQL internally from validated parameters. Identifiers (table, schema,
  columns) are validated against a regular expression before being composed into SQL.
- The WHERE clause is accepted as free text (necessary for flexibility), but DML is impossible
  since the command is always built as `SELECT TOP n ... FROM [t] WHERE ...`.
- `max_query_rows` caps the maximum number of rows returned per database (default 10,000).
  The `top` parameter in `query_table` is always clamped to this value.
- **Even so**, avoid exposing this server in untrusted environments or with sensitive data.
  Use firewalls and access policies to restrict who can execute queries via MCP. Use
  database accounts with the minimum required privileges (read-only) for all configured
  connections.
