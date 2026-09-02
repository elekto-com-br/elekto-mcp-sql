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
/// <remarks>
/// <para>
/// Optional parameters are declared as non-nullable types with a sentinel default (an empty string,
/// or zero) rather than as <c>string?</c> / <c>decimal?</c>. This looks like a stylistic quirk and is
/// not: a nullable parameter is published in the tool schema as the union type
/// <c>["string", "null"]</c>, and MCP clients that cannot represent a union collapse the whole
/// property to <c>{}</c> — no type, no description, no example. The caller is then told nothing at
/// all about a parameter it is expected to fill in, and guesses; guessing a JSON array where a
/// comma-separated string was wanted is the common outcome. A plain <c>{"type": "string"}</c>
/// survives every client, so the description reaches the caller that needs it.
/// </para>
/// <para>
/// Failures come back as content rather than as exceptions — see <see cref="ToolResponse"/> for why.
/// </para>
/// </remarks>
[McpServerToolType]
public sealed class SqlTools
{
    private readonly ConnectionConfig _config;

    public SqlTools(ConnectionConfig config) => _config = config;

    private SchemaReader GetReader(string database)
    {
        if (!_config.Databases.TryGetValue(database, out var entry))
        {
            var available = string.Join(", ", _config.Databases.Keys);
            throw new ToolInputException(
                $"No database named '{database}' is registered.",
                $"Registered databases are: {available}. Call list_databases to see them with their limits.",
                new { database = _config.Databases.Keys.FirstOrDefault() ?? "your-database-name" });
        }
        return new SchemaReader(entry.ConnectionString, entry.DefaultTimeoutSeconds);
    }

    private int GetMaxRows(string database) =>
        _config.Databases.TryGetValue(database, out var e) ? e.MaxQueryRows : 10_000;

    /// <summary>Empty means "not supplied"; the readers expect null for that.</summary>
    private static string? Optional(string value) =>
        string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    [McpServerTool, Description(
        "Lists the databases registered in the MCP server configuration. " +
        "Use this tool first to discover which databases are available.")]
    public string list_databases() => ToolResponse.Guard(nameof(list_databases), () =>
    {
        var entries = _config.Databases.Select(kv => new
        {
            name = kv.Key,
            max_query_rows = kv.Value.MaxQueryRows,
            default_timeout_seconds = kv.Value.DefaultTimeoutSeconds
        });
        return System.Text.Json.JsonSerializer.Serialize(entries);
    });

    [McpServerTool, Description(
        "Returns a summary overview of a database: real name, connected user, server machine, " +
        "instance name, table/view/procedure/function/schema counts and total allocated size in MB. " +
        "Use this after list_databases to quickly understand a database before exploring its objects.")]
    public string get_database_overview(
        [Description("Name of the database as registered in the configuration.")]
        string database)
        => ToolResponse.Guard(nameof(get_database_overview), () => GetReader(database).GetDatabaseOverview());

    [McpServerTool, Description(
        "Lists the available schemas in a SQL Server database, " +
        "excluding system schemas. Returns the schema name and owner.")]
    public string list_schemas(
        [Description("Name of the database as registered in the configuration.")] string database)
        => ToolResponse.Guard(nameof(list_schemas), () => GetReader(database).ListSchemas());

    [McpServerTool, Description(
        "Lists user tables with schema, approximate row count, creation/modification dates and " +
        "estimated size. Narrow large databases with schema and name_pattern rather than listing " +
        "everything.")]
    public string list_tables(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Filter by schema name. Empty means every schema. Example: 'Feeder'")]
        string schema = "",
        [Description("Filter by table name. A pattern without % matches anywhere in the name, so " +
                     "'Security' finds GenericSecurity; add % yourself for a prefix or suffix match, " +
                     "as in 'Anbima%'. Empty means every table.")]
        string name_pattern = "")
        => ToolResponse.Guard(nameof(list_tables),
            () => GetReader(database).ListTables(Optional(schema), Optional(name_pattern)));

    [McpServerTool, Description(
        "Returns the full schema of a table or view: columns with an unambiguous type_declaration " +
        "(such as 'nvarchar(250)'), max_length_chars, every extended property, computed-column " +
        "definitions and whether they are persisted; plus primary keys, foreign keys, checks, " +
        "unique constraints and indexes with their key column order and declared key width. " +
        "Note that max_length is the raw sys.columns value, in BYTES — use type_declaration or " +
        "max_length_chars to reason about how much text a column holds.")]
    public string get_table_schema(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Table name, bare, with no schema prefix. Example: 'GenericSecurity'")]
        string table,
        [Description("Table schema. Empty searches every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(get_table_schema),
            () => GetReader(database).GetTableSchema(table, Optional(schema)));

    [McpServerTool, Description(
        "Finds every column whose name matches a pattern, across all tables and (by default) views. " +
        "Use it to answer 'which objects have this column?' before a rename, a widening or an " +
        "impact review.")]
    public string find_columns(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Part of a column name. A pattern without % matches anywhere in the name, so " +
                     "'Date' finds ReferenceDate and AuxDate; add % yourself for a prefix or suffix " +
                     "match, as in 'Aux%'. Example: 'ReferenceDate'")]
        string column_pattern,
        [Description("Restrict to one schema. Empty means every schema. Example: 'Feeder'")]
        string schema = "",
        [Description("Include views as well as tables. True by default.")]
        bool include_views = true)
        => ToolResponse.Guard(nameof(find_columns),
            () => GetReader(database).FindColumns(column_pattern, Optional(schema), include_views));

    [McpServerTool, Description(
        "Lists user views. Narrow large databases with schema and name_pattern.")]
    public string list_views(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Filter by schema name. Empty means every schema. Example: 'Feeder'")]
        string schema = "",
        [Description("Filter by view name. A pattern without % matches anywhere in the name; add % " +
                     "yourself for a prefix or suffix match. Empty means every view.")]
        string name_pattern = "")
        => ToolResponse.Guard(nameof(list_views),
            () => GetReader(database).ListViews(Optional(schema), Optional(name_pattern)));

    [McpServerTool, Description(
        "Returns the DDL definition (CREATE VIEW) and columns of a view, with the same column " +
        "detail as get_table_schema.")]
    public string get_view_definition(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("View name, bare, with no schema prefix.")]
        string view,
        [Description("View schema. Empty searches every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(get_view_definition),
            () => GetReader(database).GetViewDefinition(view, Optional(schema)));

    [McpServerTool, Description(
        "Lists all user stored procedures in a database, with basic complexity metrics.")]
    public string list_procedures(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Filter by schema name. Empty means every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(list_procedures), () => GetReader(database).ListProcedures(Optional(schema)));

    [McpServerTool, Description(
        "Returns the definition text (CREATE PROCEDURE) of a stored procedure.")]
    public string get_procedure_definition(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Stored procedure name, bare, with no schema prefix.")]
        string procedure,
        [Description("Procedure schema. Empty searches every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(get_procedure_definition),
            () => GetReader(database).GetProcedureDefinition(procedure, Optional(schema)));

    [McpServerTool, Description(
        "Lists all user-defined functions (scalar, inline table-valued, " +
        "multi-statement table-valued) in a database.")]
    public string list_functions(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Filter by schema name. Empty means every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(list_functions), () => GetReader(database).ListFunctions(Optional(schema)));

    [McpServerTool, Description(
        "Returns the definition text (CREATE FUNCTION) of a user-defined function.")]
    public string get_function_definition(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Function name, bare, with no schema prefix.")]
        string function,
        [Description("Function schema. Empty searches every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(get_function_definition),
            () => GetReader(database).GetFunctionDefinition(function, Optional(schema)));

    [McpServerTool, Description(
        "Executes a SELECT on a table or view. Supports filtering, grouping, secure aggregates, " +
        "sorting, pagination and sampling. Returns an object carrying the rows plus row_count and " +
        "a truncated flag, so a short result is never mistaken for a complete one. " +
        "Does not execute DML (INSERT/UPDATE/DELETE) or stored procedures.")]
    public string query_table(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Table or view name, bare, with no schema prefix. Example: 'GenericSecurity'")]
        string table,
        [Description("Table/view schema. Empty means dbo. Example: 'Feeder'")]
        string schema = "",
        [Description("Columns as ONE comma-separated string, not a JSON array. " +
                     "Example: 'ReferenceDate, Source, Close'. Empty or '*' returns every column. " +
                     "Reserved words need no quoting or brackets.")]
        string columns = "",
        [Description("WHERE clause without the WHERE keyword, as SQL. " +
                     "Example: \"Source = 'BDS' AND ReferenceDate >= '2026-01-01'\"")]
        string where = "",
        [Description("ORDER BY clause without the ORDER BY keyword. Example: 'ReferenceDate DESC, Name'")]
        string order_by = "",
        [Description("Maximum number of rows to return (default 100, capped by the per-database limit).")]
        int top = 100,
        [Description("Number of rows to skip before returning results (for pagination, default 0).")]
        int skip = 0,
        [Description("GROUP BY columns as ONE comma-separated string. Example: 'Source, Name'")]
        string group_by = "",
        [Description("Aggregates as ONE comma-separated string of FUNC(column) [AS alias], with FUNC " +
                     "one of COUNT, SUM, AVG, MIN, MAX. Only a bare column name is allowed inside the " +
                     "parentheses. Example: 'COUNT(*) AS Total, MAX(ReferenceDate) AS Ultima'")]
        string aggregates = "",
        [Description("Random sampling percentage, from 0.01 to 100. Zero (the default) means no sampling.")]
        decimal sample_percent = 0)
        => ToolResponse.Guard(nameof(query_table), () => GetReader(database).QueryTable(
            table,
            Optional(schema),
            Optional(columns),
            Optional(where),
            Optional(order_by),
            top,
            skip,
            GetMaxRows(database),
            Optional(group_by),
            Optional(aggregates),
            sample_percent > 0 ? sample_percent : null));

    [McpServerTool, Description(
        "Returns a schema-level summary useful for initial exploration and refactoring planning: " +
        "object counts, approximate rows and estimated data/index size per schema.")]
    public string get_schema_summary(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Filter by schema name. Empty means every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(get_schema_summary), () => GetReader(database).GetSchemaSummary(Optional(schema)));

    [McpServerTool, Description(
        "Returns dependency edges between database objects. Includes foreign key dependencies " +
        "between tables and SQL-expression dependencies among views, procedures and functions.")]
    public string get_dependency_graph(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Filter by schema name. Empty means every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(get_dependency_graph), () => GetReader(database).GetDependencyGraph(Optional(schema)));

    [McpServerTool, Description(
        "Returns usages and references of a table across foreign keys and SQL modules (views, procedures, functions).")]
    public string get_table_usage(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Table name, bare, with no schema prefix.")]
        string table,
        [Description("Table schema. Empty searches every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(get_table_usage), () => GetReader(database).GetTableUsage(table, Optional(schema)));

    [McpServerTool, Description(
        "Returns a lightweight data profile by column: null ratio, distinct count, min/max values " +
        "and top frequent values. Note that this reads actual row values.")]
    public string get_data_profile(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Table name, bare, with no schema prefix.")]
        string table,
        [Description("Table schema. Empty means dbo. Example: 'Feeder'")]
        string schema = "",
        [Description("Columns to profile as ONE comma-separated string, not a JSON array. " +
                     "Example: 'Source, Name'. Empty profiles every column.")]
        string columns = "",
        [Description("Top frequent values to return per column (default 5).")]
        int top_values = 5)
        => ToolResponse.Guard(nameof(get_data_profile),
            () => GetReader(database).GetDataProfile(table, Optional(schema), Optional(columns), top_values));

    [McpServerTool, Description(
        "Returns index-health diagnostics by schema: duplicate index candidates, unused indexes and missing-index suggestions.")]
    public string get_index_health(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Filter by schema name. Empty means every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(get_index_health), () => GetReader(database).GetIndexHealth(Optional(schema)));

    [McpServerTool, Description(
        "Compares schemas between two configured databases, returning table and column differences " +
        "with each column's type_declaration, so a difference reads as 'nvarchar(250) vs nvarchar(50)'.")]
    public string compare_schemas(
        [Description("Source database name as registered in configuration.")]
        string source_database,
        [Description("Target database name as registered in configuration.")]
        string target_database,
        [Description("Source schema filter. Empty means every schema.")]
        string source_schema = "",
        [Description("Target schema filter. Empty means every schema.")]
        string target_schema = "")
        => ToolResponse.Guard(nameof(compare_schemas), () => SchemaReader.CompareSchemas(
            GetReader(source_database),
            GetReader(target_database),
            Optional(source_schema),
            Optional(target_schema)));

    [McpServerTool, Description(
        "Generates a Graphviz DOT dependency graph and node metadata for database objects.")]
    public string generate_dependency_dot(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Filter by schema name. Empty means every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(generate_dependency_dot), () => GetReader(database).GenerateDependencyDot(Optional(schema)));
}
