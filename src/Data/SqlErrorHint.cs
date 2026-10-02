// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

using Microsoft.Data.SqlClient;

namespace Elekto.Mcp.Sql.Data;

/// <summary>
/// Turns a SQL Server error into the one sentence that tells the caller what kind of problem it is.
/// </summary>
/// <remarks>
/// A permission error, a name that does not exist and a WHERE clause that does not parse call for
/// three different next steps, and the server message alone does not always make clear which one
/// applies, least of all to a caller that cannot see the database. The error number does.
/// </remarks>
internal static class SqlErrorHint
{
    /// <summary>
    /// 229 denied on an object, 230 denied on a column, 262 denied in the database, 297 no permission
    /// for the action, 300 a server permission such as VIEW SERVER STATE, 916 the login cannot access
    /// the database, 15562 a module the server will not trust in this security context.
    /// </summary>
    private static readonly HashSet<int> PermissionErrors = [229, 230, 262, 297, 300, 916, 15562];

    /// <summary>208 invalid object name, 207 invalid column name.</summary>
    private static readonly HashSet<int> NameErrors = [207, 208];

    /// <summary>102 incorrect syntax near, 156 incorrect syntax near a keyword, 4145 non-boolean expression.</summary>
    private static readonly HashSet<int> SyntaxErrors = [102, 156, 4145];

    public const string Permission =
        "This is a permission error: SQL Server refused the login, and the object may well exist. "
        + "Call check_permissions for what this login is missing and the GRANT statements that fix it.";

    public const string Name =
        "A table, view or column name does not exist — or exists but this login cannot see it, since "
        + "SQL Server hides objects the login holds no permission on. Check the name with list_tables or "
        + "get_table_schema; if it should be there, check_permissions says what the login can see.";

    public const string Syntax =
        "SQL Server could not parse the statement. The 'where' and 'order_by' clauses are passed as "
        + "written, so a typo or a quoting mistake in either surfaces here.";

    public const string Unknown =
        "The parameters were well-formed, so this came from the database itself. Check the object "
        + "exists with list_tables or get_table_schema before querying it.";

    /// <summary>The hint for the first error the exception carries that falls in a known kind.</summary>
    public static string For(SqlException ex) => For(Numbers(ex));

    internal static string For(IEnumerable<int> numbers)
    {
        foreach (var number in numbers)
        {
            if (PermissionErrors.Contains(number)) return Permission;
            if (NameErrors.Contains(number)) return Name;
            if (SyntaxErrors.Contains(number)) return Syntax;
        }
        return Unknown;
    }

    private static IEnumerable<int> Numbers(SqlException ex) =>
        ex.Errors.Count > 0 ? ex.Errors.Cast<SqlError>().Select(e => e.Number) : [ex.Number];
}
