// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

namespace Elekto.Mcp.Sql.Tools;

/// <summary>
/// The server instructions an MCP client hands the model when it connects: how the tools fit
/// together, which no single tool description can say.
/// </summary>
internal static class ServerInstructions
{
    public const string Usage =
        "Read-only access to the SQL Server databases configured for this server. Start with " +
        "list_databases: every other tool takes one of its names as 'database'. Then " +
        "get_database_overview to size a database up; list_tables, list_views, list_procedures, " +
        "list_functions and find_columns to locate objects; get_table_schema and the get_*_definition " +
        "tools for their structure and code; get_table_usage and get_dependency_graph for impact " +
        "analysis; query_table and get_data_profile to read data. SQL Server leaves out, without any " +
        "error, objects the login has no permission on, so when a result looks short call " +
        "check_permissions. A failure comes back as content with ok: false, an error, a hint and " +
        "usually an example; correct the call from them rather than retrying it unchanged.";

    /// <summary>The instructions, with the configuration problem first when there is one.</summary>
    public static string For(string? configurationProblem) =>
        configurationProblem is null ? Usage : configurationProblem + "\n\n" + Usage;
}
