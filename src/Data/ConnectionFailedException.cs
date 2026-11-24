// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

using Microsoft.Data.SqlClient;

namespace Elekto.Mcp.Sql.Data;

/// <summary>
/// The connection to SQL Server could not be opened, so the call failed before any query ran.
/// </summary>
/// <remarks>
/// <see cref="SchemaReader"/> raises it in place of the exception that opening the connection
/// threw. Only that code knows the error came while connecting, and it matters: a login
/// timeout and a query timeout share the number -2, and a server out of reach is no reason to
/// question the call. The message and the hint say which it was.
/// </remarks>
public sealed class ConnectionFailedException : Exception
{
    /// <summary>What the caller should do about it, in a sentence or two.</summary>
    public string Hint { get; }

    public ConnectionFailedException(SqlException inner)
        : base(SqlErrorHint.Describe(inner, connecting: true), inner)
    {
        Hint = SqlErrorHint.For(inner, connecting: true);
    }

    /// <summary>
    /// For what SqlClient reports as an <see cref="InvalidOperationException"/> instead. Seen when
    /// two calls reach for a server that does not answer at once: the pool makes the second wait for
    /// the first one's attempt, gives up when the timeout runs out, and blames a full pool.
    /// </summary>
    public ConnectionFailedException(InvalidOperationException inner)
        : base($"{SqlErrorHint.CouldNotConnect}: {inner.Message}", inner)
    {
        Hint = SqlErrorHint.Connection;
    }
}
