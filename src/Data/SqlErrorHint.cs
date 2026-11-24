// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

using Microsoft.Data.SqlClient;

namespace Elekto.Mcp.Sql.Data;

/// <summary>
/// Turns a SQL Server error into the one sentence that tells the caller what kind of problem it is,
/// and the line that states the error.
/// </summary>
/// <remarks>
/// <para>
/// A permission error, a name that does not exist and a WHERE clause that does not parse call for
/// three different next steps, and the server message alone does not always make clear which one
/// applies, least of all to a caller that cannot see the database. The error number does.
/// </para>
/// <para>
/// A connection that could not be made calls for none of them: nothing in the call is wrong, and a
/// caller told that SQL Server rejected its query goes looking for a mistake that is not there. The
/// number alone does not always reveal it: SqlClient numbers a timeout -2 whether it came while
/// connecting or while a query ran, and not every failure to connect carries one of the numbers
/// below. So the code that opens the connection says so (<c>connecting</c>), and every error raised
/// there counts as a failure to connect.
/// </para>
/// </remarks>
internal static class SqlErrorHint
{
    private enum Kind { Permission, Name, Syntax, Login, Connection, Timeout, Unknown }

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

    /// <summary>
    /// The server answered, but refused the login or the database the connection asks for: 4060 cannot
    /// open the database requested by the login, 4064 cannot open the login's default database, 15113
    /// account locked out by password validation, 18452 login from an untrusted domain, 18456 login
    /// failed, 18470 account disabled, 18486 account locked out, 18487 password expired, 18488
    /// password must be changed.
    /// </summary>
    private static readonly HashSet<int> LoginErrors =
        [4060, 4064, 15113, 18452, 18456, 18470, 18486, 18487, 18488];

    /// <summary>
    /// The network failed, before reaching the server or after. SqlClient on Windows numbers these
    /// errors with the Windows or Winsock code: 2 and 53 no named pipe at that address, 64 the network
    /// name is no longer available, 121 semaphore timeout, 232 the pipe is being closed, 233 no
    /// process on the other end of the pipe, 258 wait timeout, 1225 the remote computer refused the
    /// connection, 1231 and 1232 network or host unreachable, 1236 connection aborted by this machine,
    /// 10050 network down, 10051 network unreachable, 10053 connection aborted, 10054 connection reset
    /// by the server, 10060 timed out, 10061 refused, 10064 host down, 10065 no route to host, 11001
    /// host not found. A login refused over named pipes can arrive as 233 too, which is why
    /// <see cref="Connection"/> also points to the login when the message says the connection was made.
    /// </summary>
    private static readonly HashSet<int> NetworkErrors =
    [
        2, 53, 64, 121, 232, 233, 258, 1225, 1231, 1232, 1236,
        10050, 10051, 10053, 10054, 10060, 10061, 10064, 10065, 11001
    ];

    /// <summary>SqlClient's number for a timeout, of the connection and of a command alike.</summary>
    private const int TimeoutExpired = -2;

    /// <summary>How the error line of a failure to open the connection begins.</summary>
    public const string CouldNotConnect = "Could not connect to SQL Server";

    public const string Permission =
        "This is a permission error: the login connected, but SQL Server denied it this action, and the "
        + "object may well exist. Call check_permissions for what this login is missing and the GRANT "
        + "statements that fix it.";

    public const string Name =
        "A table, view or column name does not exist, or exists but this login cannot see it, since "
        + "SQL Server hides objects the login holds no permission on. Check the name with list_tables or "
        + "get_table_schema; if it should be there, check_permissions says what the login can see.";

    public const string Syntax =
        "SQL Server could not parse the statement. The 'where' and 'order_by' clauses are passed as "
        + "written, so a typo or a quoting mistake in either surfaces here.";

    public const string Login =
        "SQL Server answered, but refused the login or the database that this connection's configuration "
        + "names, so nothing in the call is wrong. Tell the user to check the User ID, the password and "
        + "the Initial Catalog (or Database) of the connection string, and whether the login is disabled, "
        + "locked out or has an expired password. check_permissions cannot help: it needs to connect too.";

    public const string Connection =
        "The connection to SQL Server failed, so nothing in the call is wrong. Most often nothing answered "
        + "at the address in this connection's Data Source: the server, or a tunnel or VPN on the way to "
        + "it, is down or closed. If the message says a connection was established and the login then "
        + "failed, look at the login in the connection string instead. Tell the user, and repeat the call "
        + "unchanged once the connection works; until then every tool on this connection fails the same way.";

    public const string Timeout =
        "SQL Server did not answer within this connection's command timeout (default_timeout_seconds, which "
        + "list_databases shows): the query was too slow, or the server too busy or out of reach. Narrow "
        + "the call, with a schema filter or with 'where' and 'top' on query_table, or ask the user to "
        + "raise default_timeout_seconds in the configuration.";

    public const string Unknown =
        "The parameters were well-formed, so this came from the database itself. Check the object "
        + "exists with list_tables or get_table_schema before querying it.";

    /// <summary>The hint for the first error the exception carries that falls in a known kind.</summary>
    /// <param name="connecting">Whether the error came while opening the connection, before any query ran.</param>
    public static string For(SqlException ex, bool connecting = false) => For(Numbers(ex), connecting);

    internal static string For(IEnumerable<int> numbers, bool connecting = false) =>
        Classify(numbers, connecting) switch
        {
            Kind.Permission => Permission,
            Kind.Name => Name,
            Kind.Syntax => Syntax,
            Kind.Login => Login,
            Kind.Connection => Connection,
            Kind.Timeout => Timeout,
            _ => Unknown
        };

    /// <summary>
    /// The error line: what happened, the error number and SQL Server's own message. A failure to
    /// connect, a lost connection and a timeout each say so, rather than that SQL Server rejected the
    /// query.
    /// </summary>
    /// <param name="connecting">Whether the error came while opening the connection, before any query ran.</param>
    /// <param name="target">The object the query read, such as <c>[dbo].[Orders]</c>, when there is one to name.</param>
    public static string Describe(SqlException ex, bool connecting = false, string? target = null)
    {
        var on = target is null ? "" : $" on {target}";
        var what = Classify(Numbers(ex), connecting) switch
        {
            Kind.Login => CouldNotConnect,
            Kind.Connection when connecting => CouldNotConnect,
            Kind.Connection when target is null => "Lost the connection to SQL Server",
            Kind.Connection => $"Lost the connection to SQL Server while querying {target}",
            Kind.Timeout => $"The query{on} timed out",
            _ => $"SQL Server rejected the query{on}"
        };
        return $"{what} (error {ex.Number}): {ex.Message}";
    }

    /// <summary>
    /// The kind of the first error that falls in one. While connecting, whatever is not a refused login
    /// is a failed connection: no query has run that could be at fault.
    /// </summary>
    private static Kind Classify(IEnumerable<int> numbers, bool connecting)
    {
        foreach (var number in numbers)
        {
            if (LoginErrors.Contains(number)) return Kind.Login;
            if (NetworkErrors.Contains(number)) return Kind.Connection;
            if (connecting) continue;
            if (number == TimeoutExpired) return Kind.Timeout;
            if (PermissionErrors.Contains(number)) return Kind.Permission;
            if (NameErrors.Contains(number)) return Kind.Name;
            if (SyntaxErrors.Contains(number)) return Kind.Syntax;
        }
        return connecting ? Kind.Connection : Kind.Unknown;
    }

    private static IEnumerable<int> Numbers(SqlException ex) =>
        ex.Errors.Count > 0 ? ex.Errors.Cast<SqlError>().Select(e => e.Number) : [ex.Number];
}
