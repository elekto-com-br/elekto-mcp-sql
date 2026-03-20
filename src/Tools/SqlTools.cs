// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

using System.ComponentModel;
using Elekto.Mcp.Sql.Configuration;
using Elekto.Mcp.Sql.Data;
using ModelContextProtocol.Server;

namespace Elekto.Mcp.Sql.Tools;

/// <summary>
/// MCP tools for SQL Server introspection and querying.
/// All operations are read-only.
/// </summary>
[McpServerToolType]
public sealed class SqlTools
{
    private readonly ConnectionConfig _config;

    public SqlTools(ConnectionConfig config)
    {
        _config = config;
    }

    private SchemaReader GetReader(string database)
    {
        if (!_config.Databases.TryGetValue(database, out var entry))
        {
            var available = string.Join(", ", _config.Databases.Keys);
            throw new ArgumentException(
                $"Database '{database}' not found. Available: {available}");
        }
        return new SchemaReader(entry.ConnectionString);
    }

    private int GetMaxRows(string database) =>
        _config.Databases.TryGetValue(database, out var e) ? e.MaxQueryRows : 10_000;

    [McpServerTool, Description(
        "Lists the databases registered in the MCP server configuration. " +
        "Use this tool first to discover which databases are available.")]
    public string list_databases()
    {
        var entries = _config.Databases.Select(kv => new
        {
            name         = kv.Key,
            max_query_rows = kv.Value.MaxQueryRows
        });
        return System.Text.Json.JsonSerializer.Serialize(entries);
    }

    [McpServerTool, Description(
        "Returns a summary overview of a database: real name, connected user, server machine, " +
        "instance name, table/view/procedure/function/schema counts and total allocated size in MB. " +
        "Use this after list_databases to quickly understand a database before exploring its objects.")]
    public string get_database_overview(
        [Description("Name of the database as registered in the configuration.")]
        string database)
        => GetReader(database).GetDatabaseOverview();

    [McpServerTool, Description(
        "Lists the available schemas in a SQL Server database, " +
        "excluding system schemas. Returns the schema name and owner.")]
    public string list_schemas(
        [Description("Name of the database as registered in the configuration.")] string database)
        => GetReader(database).ListSchemas();

    [McpServerTool, Description(
        "Lists all user tables in a database, with schema and approximate row count.")]
    public string list_tables(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Filter by schema name (optional).")]
        string? schema = null)
        => GetReader(database).ListTables(schema);

    [McpServerTool, Description(
        "Returns the full schema of a table: columns (type, nullable, identity, " +
        "default value, description), primary keys, foreign keys and indexes.")]
    public string get_table_schema(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Table name.")]
        string table,
        [Description("Table schema (optional, defaults to dbo).")]
        string? schema = null)
        => GetReader(database).GetTableSchema(table, schema);

    [McpServerTool, Description(
        "Lists all user views in a database.")]
    public string list_views(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Filter by schema name (optional).")]
        string? schema = null)
        => GetReader(database).ListViews(schema);

    [McpServerTool, Description(
        "Returns the DDL definition (CREATE VIEW) and columns of a view.")]
    public string get_view_definition(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("View name.")]
        string view,
        [Description("View schema (optional, defaults to dbo).")]
        string? schema = null)
        => GetReader(database).GetViewDefinition(view, schema);

    [McpServerTool, Description(
        "Lists all user stored procedures in a database.")]
    public string list_procedures(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Filter by schema name (optional).")]
        string? schema = null)
        => GetReader(database).ListProcedures(schema);

    [McpServerTool, Description(
        "Returns the definition text (CREATE PROCEDURE) of a stored procedure.")]
    public string get_procedure_definition(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Stored procedure name.")]
        string procedure,
        [Description("Procedure schema (optional, defaults to dbo).")]
        string? schema = null)
        => GetReader(database).GetProcedureDefinition(procedure, schema);

    [McpServerTool, Description(
        "Lists all user-defined functions (scalar, inline table-valued, " +
        "multi-statement table-valued) in a database.")]
    public string list_functions(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Filter by schema name (optional).")]
        string? schema = null)
        => GetReader(database).ListFunctions(schema);

    [McpServerTool, Description(
        "Returns the definition text (CREATE FUNCTION) of a user-defined function.")]
    public string get_function_definition(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Function name.")]
        string function,
        [Description("Function schema (optional, defaults to dbo).")]
        string? schema = null)
        => GetReader(database).GetFunctionDefinition(function, schema);

    [McpServerTool, Description(
        "Executes a SELECT on a table or view. Supports filtering, sorting and pagination. " +
        "Maximum rows are configurable per database (default 10,000). " +
        "Use 'top' and 'skip' to sample large tables without overloading the server. " +
        "Does not execute DML (INSERT/UPDATE/DELETE) or stored procedures.")]
    public string query_table(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Table or view name.")]
        string table,
        [Description("Table/view schema (optional, defaults to dbo).")]
        string? schema = null,
        [Description("Comma-separated list of columns. Use * for all (default).")]
        string? columns = null,
        [Description("WHERE clause without the WHERE keyword. Ex: 'Status = 1 AND DataRef >= ''2024-01-01'''")]
        string? where = null,
        [Description("ORDER BY clause without the ORDER BY keyword. Ex: 'DataRef DESC'")]
        string? order_by = null,
        [Description("Maximum number of rows to return (default 100, capped by the per-database limit).")]
        int top = 100,
        [Description("Number of rows to skip before returning results (for pagination, default 0).")]
        int skip = 0)
        => GetReader(database).QueryTable(table, schema, columns, where, order_by, top, skip, GetMaxRows(database));
}
