# Elekto.Mcp.Sql

Read-only MCP server for SQL Server 2022+ introspection and querying.
Exposes schema metadata, object definitions, and data queries via the MCP protocol (stdio),
allowing GitHub Copilot (and other MCP clients) to understand your database structure
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

**Regardless of the precautions you take, the responsibility for any consequences arising
from the use of this tool rests entirely with you.** This software is provided *as is*
with no warranties of any kind.

## Available Tools

| Tool | Description |
|------|-------------|
| `list_databases` | Databases registered in the configuration |
| `get_database_overview` | High-level database summary (counts, size, connection metadata) |
| `get_schema_summary` | Aggregated metrics by schema (objects, rows, size) |
| `list_schemas` | Schemas in a database (excluding system schemas) |
| `list_tables` | User tables with schema, dates, approximate rows and estimated size |
| `list_views` | User views |
| `list_procedures` | User stored procedures (with basic complexity metrics) |
| `list_functions` | User-defined functions (with basic complexity metrics) |
| `get_table_schema` | Columns, PKs, FKs, checks, uniques, indexes and computed/collation metadata |
| `get_view_definition` | DDL definition + columns of a view |
| `get_procedure_definition` | CREATE PROCEDURE text |
| `get_function_definition` | CREATE FUNCTION text |
| `get_dependency_graph` | Object dependency edges (FK + SQL dependencies) |
| `get_table_usage` | References to a table across FKs and SQL modules |
| `get_data_profile` | Column profile (null ratio, distinct count, min/max, top values) |
| `get_index_health` | Duplicate/unused index diagnostics + missing-index suggestions |
| `compare_schemas` | Compares table/column structure between two configured databases |
| `generate_dependency_dot` | Graphviz DOT dependency graph with node metadata (`node_kind`) |
| `query_table` | SELECT from a table or view with filtering, grouping, secure aggregates, sorting, sampling and pagination |

## Configuration

Configuration is provided via the `MCP_SQL_CONNECTIONS` environment variable,
as a JSON object mapping database names to their configurations.

### Simple format (direct connection string)

```json
{
  "RiskSystem": "Server=.\\DEV;Database=RiskSystem;Integrated Security=SSPI"
}
```

### Full format (with options)

```json
{
  "RiskSystem": {
    "connection_string": "Server=.\\DEV;Database=RiskSystem;Integrated Security=SSPI",
    "max_query_rows": 10000,
    "default_timeout_seconds": 30
  },
  "Reports": {
    "connection_string": "Server=.\\PROD;Database=Reports;User Id=%{DB_USER};Password=%{DB_PASS}",
    "max_query_rows": 1000,
    "default_timeout_seconds": 60
  }
}
```

### Environment variable expansion

Use `%{VARIABLE_NAME}` inside connection strings to avoid storing credentials in plain text.
Variables are resolved from the process environment at server startup.

Example: `%{DB_PASS}` is replaced by the value of `$env:DB_PASS`.

If a referenced variable does not exist, the server fails with an explicit error message.

## Visual Studio 2026 Setup (.mcp.json)

Create or edit `.mcp.json` at the solution root (or in your user profile for global use):

```json
{
  "servers": {
    "sql": {
      "type": "stdio",
      "command": "dotnet",
      "args": ["D:\\Tools\\Elekto.Mcp.Sql\\Elekto.Mcp.Sql.dll"],
      "env": {
        "MCP_SQL_CONNECTIONS": "{\"RiskSystem\": {\"connection_string\": \"Server=.\\\\DEV;Database=RiskSystem;Integrated Security=SSPI\"}}"
      }
    }
  }
}
```

Tips:
- Backslashes inside JSON require double escaping (`\\\\` in JSON-within-JSON).
- For connection strings with credentials, prefer environment variables:
  ```json
  "env": {
    "MCP_SQL_CONNECTIONS": "{\"DB\": {\"connection_string\": \"...User Id=%{DB_USER};Password=%{DB_PASS}\"}}",
    "DB_USER": "user",
    "DB_PASS": "%{PASSWORD_IN_SYSTEM}"
  }
  ```
  Or set `DB_USER` and `DB_PASS` directly in the OS environment, without declaring them
  in `.mcp.json`.

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
