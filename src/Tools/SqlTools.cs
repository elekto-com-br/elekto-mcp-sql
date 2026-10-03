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
/// <para>
/// With no connection configured every tool still answers, with the steps to configure one; see
/// <see cref="ConnectionRegistry"/>.
/// </para>
/// </remarks>
[McpServerToolType]
public sealed class SqlTools
{
    private readonly ConnectionRegistry _connections;

    public SqlTools(ConnectionRegistry connections) => _connections = connections;

    private SchemaReader GetReader(string database)
    {
        var config = _connections.GetConfig();
        if (!config.Databases.TryGetValue(database, out var entry))
        {
            var available = string.Join(", ", config.Databases.Keys);
            throw new ToolInputException(
                $"No database named '{database}' is registered.",
                $"Registered databases are: {available}. Call list_databases to see them with their limits.",
                new { database = config.Databases.Keys.FirstOrDefault() ?? "your-database-name" });
        }
        return new SchemaReader(entry.ConnectionString, entry.DefaultTimeoutSeconds);
    }

    private int GetMaxRows(string database) =>
        _connections.GetConfig().Databases.TryGetValue(database, out var e) ? e.MaxQueryRows : 10_000;

    /// <summary>Empty means "not supplied"; the readers expect null for that.</summary>
    private static string? Optional(string value) =>
        string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    [McpServerTool(Title = "List configured databases", ReadOnly = true, Destructive = false, Idempotent = true, OpenWorld = false)]
    [Description(
        "Lists the logical database names this server is configured to reach, with each one's row " +
        "limit and command timeout. Call it first: every other tool, starting with get_database_overview, " +
        "takes one of these names as its 'database' argument. It reads only the server's configuration and never connects to SQL " +
        "Server; when nothing is configured it answers ok: false with the file to create, where the " +
        "server looked and an example.")]
    public string list_databases() => ToolResponse.Guard(nameof(list_databases), () =>
    {
        var entries = _connections.GetConfig().Databases.Select(kv => new
        {
            name = kv.Key,
            max_query_rows = kv.Value.MaxQueryRows,
            default_timeout_seconds = kv.Value.DefaultTimeoutSeconds
        });
        return System.Text.Json.JsonSerializer.Serialize(entries);
    });

    [McpServerTool(Title = "Database overview", ReadOnly = true, Destructive = false, Idempotent = true, OpenWorld = false)]
    [Description(
        "Summarizes one database: real name, server and instance, connected login, counts of tables, " +
        "views, procedures, functions and schemas, and allocated size in MB. Use it after " +
        "list_databases to size up a database before exploring it; for the same figures per schema " +
        "use get_schema_summary. Counts cover only what the login can see: 'visibility.complete' is " +
        "false when SQL Server hides objects, and check_permissions then says which GRANT is missing.")]
    public string get_database_overview(
        [Description("Name of the database as registered in the configuration.")]
        string database)
        => ToolResponse.Guard(nameof(get_database_overview), () => GetReader(database).GetDatabaseOverview());

    [McpServerTool(Title = "Check login permissions", ReadOnly = true, Destructive = false, Idempotent = true, OpenWorld = false)]
    [Description(
        "Reports what the connected login can and cannot see in a database: identity, roles, " +
        "effective permissions, server version and, for each group of tools, whether it sees " +
        "everything ('complete'), only part ('partial') or nothing ('unavailable'), with the missing " +
        "permission and the GRANT that adds it in this server version's syntax. Also reports " +
        "'write_access', so an account meant to be read-only can be verified rather than assumed. " +
        "Call it when a listing looks short, a definition comes back hidden, get_database_overview " +
        "reports incomplete visibility, or a tool fails with a permission error: SQL Server leaves " +
        "out what a login cannot see without raising any error.")]
    public string check_permissions(
        [Description("Name of the database as registered in the configuration.")]
        string database)
        => ToolResponse.Guard(nameof(check_permissions), () => GetReader(database).CheckPermissions());

    [McpServerTool(Title = "List schemas", ReadOnly = true, Destructive = false, Idempotent = true, OpenWorld = false)]
    [Description(
        "Lists the user schemas of a database with their owners, leaving out system schemas. Use it " +
        "to learn the names the 'schema' filter of the other tools accepts; for object counts and " +
        "sizes per schema use get_schema_summary instead.")]
    public string list_schemas(
        [Description("Name of the database as registered in the configuration.")] string database)
        => ToolResponse.Guard(nameof(list_schemas), () => GetReader(database).ListSchemas());

    [McpServerTool(Title = "List tables", ReadOnly = true, Destructive = false, Idempotent = true, OpenWorld = false)]
    [Description(
        "Lists user tables with schema, approximate row count, data and index size in MB, and " +
        "creation and modification dates. Use it to find tables by name or schema and to spot the " +
        "large ones; to find tables by a column they hold use find_columns, and for one table's " +
        "columns and keys use get_table_schema. Narrow large databases with schema and name_pattern.")]
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

    [McpServerTool(Title = "Table structure", ReadOnly = true, Destructive = false, Idempotent = true, OpenWorld = false)]
    [Description(
        "Returns the full structure of one table or view: columns with an unambiguous " +
        "type_declaration (such as 'nvarchar(250)'), max_length_chars, every extended property, " +
        "computed-column definitions and whether they are persisted; plus primary key, foreign keys, " +
        "check and unique constraints, and indexes with key column order and declared key width. Call " +
        "it before query_table to learn exact column names and types; for a view's SQL text use " +
        "get_view_definition. max_length is the raw sys.columns value in BYTES, so reason about text " +
        "length with type_declaration or max_length_chars.")]
    public string get_table_schema(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Table name, bare, with no schema prefix. Example: 'GenericSecurity'")]
        string table,
        [Description("Table schema. Empty searches every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(get_table_schema),
            () => GetReader(database).GetTableSchema(table, Optional(schema)));

    [McpServerTool(Title = "Find columns by name", ReadOnly = true, Destructive = false, Idempotent = true, OpenWorld = false)]
    [Description(
        "Finds every table and, by default, every view holding a column whose name matches a pattern, " +
        "with each column's type_declaration and nullability. Use it to answer 'which objects have " +
        "this column?' before a rename, a widening or an impact review; to find objects by their own " +
        "name use list_tables or list_views.")]
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

    [McpServerTool(Title = "List views", ReadOnly = true, Destructive = false, Idempotent = true, OpenWorld = false)]
    [Description(
        "Lists user views with their schema. Use it to find views by name or schema; for a view's " +
        "columns and SQL text use get_view_definition, and for tables use list_tables. Narrow large " +
        "databases with schema and name_pattern.")]
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

    [McpServerTool(Title = "View definition", ReadOnly = true, Destructive = false, Idempotent = true, OpenWorld = false)]
    [Description(
        "Returns the CREATE VIEW text of one view together with its columns, in the same detail as " +
        "get_table_schema. Use it to see how a view derives its data; for the objects it reads from " +
        "use get_dependency_graph. A view the login can see but not read comes back with definition " +
        "null and definition_visible false, which check_permissions explains.")]
    public string get_view_definition(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("View name, bare, with no schema prefix.")]
        string view,
        [Description("View schema. Empty searches every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(get_view_definition),
            () => GetReader(database).GetViewDefinition(view, Optional(schema)));

    [McpServerTool(Title = "List stored procedures", ReadOnly = true, Destructive = false, Idempotent = true, OpenWorld = false)]
    [Description(
        "Lists the user stored procedures the login can see, with line count, join count and number " +
        "of referenced objects as rough complexity metrics. Use it to find procedures and pick the " +
        "complex ones; for one procedure's text use get_procedure_definition. definition_visible is " +
        "false, and the metrics null, for a procedure whose text is hidden from the login; " +
        "referenced_object_count is null when the login cannot read dependencies. SQL Server leaves " +
        "out procedures the login has no permission on without an error: get_database_overview says " +
        "whether that happens, check_permissions why.")]
    public string list_procedures(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Filter by schema name. Empty means every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(list_procedures), () => GetReader(database).ListProcedures(Optional(schema)));

    [McpServerTool(Title = "Stored procedure definition", ReadOnly = true, Destructive = false, Idempotent = true, OpenWorld = false)]
    [Description(
        "Returns the CREATE PROCEDURE text of one stored procedure. Use it to read or review a " +
        "procedure found with list_procedures; this server never executes procedures. A procedure the " +
        "login can see but not read comes back with definition null and definition_visible false.")]
    public string get_procedure_definition(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Stored procedure name, bare, with no schema prefix.")]
        string procedure,
        [Description("Procedure schema. Empty searches every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(get_procedure_definition),
            () => GetReader(database).GetProcedureDefinition(procedure, Optional(schema)));

    [McpServerTool(Title = "List user functions", ReadOnly = true, Destructive = false, Idempotent = true, OpenWorld = false)]
    [Description(
        "Lists the user-defined functions the login can see (scalar, inline table-valued and " +
        "multi-statement table-valued), with the same complexity metrics and visibility flags as " +
        "list_procedures. Use it to find functions; for one function's text use " +
        "get_function_definition. SQL Server leaves out functions the login has no permission on " +
        "without an error: get_database_overview says whether that happens, check_permissions why.")]
    public string list_functions(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Filter by schema name. Empty means every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(list_functions), () => GetReader(database).ListFunctions(Optional(schema)));

    [McpServerTool(Title = "User function definition", ReadOnly = true, Destructive = false, Idempotent = true, OpenWorld = false)]
    [Description(
        "Returns the CREATE FUNCTION text of one user-defined function. Use it to read or review a " +
        "function found with list_functions; this server never executes functions. A function the " +
        "login can see but not read comes back with definition null and definition_visible false.")]
    public string get_function_definition(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Function name, bare, with no schema prefix.")]
        string function,
        [Description("Function schema. Empty searches every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(get_function_definition),
            () => GetReader(database).GetFunctionDefinition(function, Optional(schema)));

    [McpServerTool(Title = "Query a table or view", ReadOnly = true, Destructive = false, Idempotent = true, OpenWorld = false)]
    [Description(
        "Runs a SELECT on one table or view, with optional column list, filter, ordering, grouping, " +
        "aggregates (COUNT, SUM, AVG, MIN, MAX), pagination and random sampling, and returns the rows " +
        "in an object with row_count and a measured 'truncated' flag, so a short result is never " +
        "mistaken for a complete one. Rows are capped by the database's max_query_rows. Call " +
        "get_table_schema first for exact column names; for null ratios, distinct counts and frequent " +
        "values use get_data_profile instead of paging through rows. Reads actual data; never runs " +
        "INSERT, UPDATE, DELETE or procedures.")]
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

    [McpServerTool(Title = "Schema summary", ReadOnly = true, Destructive = false, Idempotent = true, OpenWorld = false)]
    [Description(
        "Summarizes each schema of a database: counts of tables, views, procedures and functions, " +
        "approximate rows and estimated data and index size. Use it to see where a database's weight " +
        "lies before exploring or planning a refactoring; for whole-database totals use " +
        "get_database_overview, and for the tables of one schema use list_tables.")]
    public string get_schema_summary(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Filter by schema name. Empty means every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(get_schema_summary), () => GetReader(database).GetSchemaSummary(Optional(schema)));

    [McpServerTool(Title = "Dependency graph", ReadOnly = true, Destructive = false, Idempotent = true, OpenWorld = false)]
    [Description(
        "Returns the dependency edges between database objects as data: foreign keys between tables, " +
        "and references among views, procedures and functions. Use it to analyze dependencies " +
        "programmatically; for a diagram use generate_dependency_dot, and for what references one " +
        "table use get_table_usage. Fails, saying so, when the login cannot read " +
        "sys.sql_expression_dependencies; without VIEW DEFINITION the module edges cover only modules " +
        "the login can read.")]
    public string get_dependency_graph(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Filter by schema name. Empty means every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(get_dependency_graph), () => GetReader(database).GetDependencyGraph(Optional(schema)));

    [McpServerTool(Title = "Table usage", ReadOnly = true, Destructive = false, Idempotent = true, OpenWorld = false)]
    [Description(
        "Lists everything that references one table: foreign keys to and from it, and the views, " +
        "procedures and functions whose code uses it. Use it for impact analysis before changing a " +
        "table; for the dependency graph of the whole database use get_dependency_graph. " +
        "sql_module_usage is null when the login cannot read dependencies, and 'visibility' says when " +
        "the list may be incomplete.")]
    public string get_table_usage(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Table name, bare, with no schema prefix.")]
        string table,
        [Description("Table schema. Empty searches every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(get_table_usage), () => GetReader(database).GetTableUsage(table, Optional(schema)));

    [McpServerTool(Title = "Profile column data", ReadOnly = true, Destructive = false, Idempotent = true, OpenWorld = false)]
    [Description(
        "Profiles the columns of one table or view: null ratio, distinct count, minimum, maximum and " +
        "most frequent values. Use it to learn how data is distributed without paging through rows " +
        "with query_table. It reads actual row values and scans the table, so on a large table it can " +
        "take a while; limit it to the columns you need.")]
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

    [McpServerTool(Title = "Index health", ReadOnly = true, Destructive = false, Idempotent = true, OpenWorld = false)]
    [Description(
        "Reports index health by schema: duplicate index candidates, unused indexes and missing-index " +
        "suggestions. Use it when reviewing indexing or slow queries; for one table's indexes use " +
        "get_table_schema. Unused and missing indexes come from server DMVs, which reset when SQL " +
        "Server restarts; without VIEW SERVER STATE they come back null and 'visibility.unavailable' " +
        "says why, while duplicates are still reported.")]
    public string get_index_health(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Filter by schema name. Empty means every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(get_index_health), () => GetReader(database).GetIndexHealth(Optional(schema)));

    [McpServerTool(Title = "Compare two databases", ReadOnly = true, Destructive = false, Idempotent = true, OpenWorld = false)]
    [Description(
        "Compares the table and column structure of two configured databases, such as development and " +
        "production: tables present in only one, columns present in only one, and columns whose type " +
        "or nullability differ, each with its type_declaration so a difference reads as " +
        "'nvarchar(250) vs nvarchar(50)'. Use it to check a migration or detect drift. It compares " +
        "tables and columns only, not views, code, indexes or data; to see one differing table in " +
        "full, call get_table_schema on each database.")]
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

    [McpServerTool(Title = "Dependency diagram (DOT)", ReadOnly = true, Destructive = false, Idempotent = true, OpenWorld = false)]
    [Description(
        "Generates a Graphviz DOT diagram of the dependencies between database objects, with node " +
        "metadata (node_kind) for styling. Use it to draw or render the dependency graph; for the " +
        "edges as data use get_dependency_graph. 'visibility' says when module references are missing " +
        "for lack of permission.")]
    public string generate_dependency_dot(
        [Description("Name of the database as registered in the configuration.")]
        string database,
        [Description("Filter by schema name. Empty means every schema. Example: 'Feeder'")]
        string schema = "")
        => ToolResponse.Guard(nameof(generate_dependency_dot), () => GetReader(database).GenerateDependencyDot(Optional(schema)));
}
