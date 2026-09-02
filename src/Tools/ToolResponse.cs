// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

using System.Text.Json;
using Elekto.Mcp.Sql.Data;
using Microsoft.Data.SqlClient;

namespace Elekto.Mcp.Sql.Tools;

/// <summary>
/// Runs a tool body and, when it fails, answers with something the caller can act on.
/// </summary>
/// <remarks>
/// <para>
/// An exception thrown out of a tool does not reach the caller. The MCP host replaces it with a
/// generic line — "An error occurred invoking 'query_table'" — which says only that something went
/// wrong. A caller that cannot read this server's source is then left guessing, and guessing costs
/// a round trip each time.
/// </para>
/// <para>
/// So a failure is returned as content instead: a JSON object carrying <c>ok: false</c>, what went
/// wrong, what to do about it, and a worked example. The trade is that the host no longer flags the
/// call as an error. For a read-only introspection server whose caller is usually a language model,
/// being readable is worth more than being flagged — an unreadable error is a failure the caller
/// repeats, and a readable one is a failure it fixes.
/// </para>
/// <para>
/// A successful result keeps the shape it always had, so <c>ok</c> appears only on failure.
/// </para>
/// </remarks>
internal static class ToolResponse
{
    /// <summary>Invokes <paramref name="action"/>, turning any failure into a readable payload.</summary>
    public static string Guard(string toolName, Func<string> action)
    {
        try
        {
            return action();
        }
        catch (ToolInputException ex)
        {
            return Failure(toolName, ex.Message, ex.Hint, ex.Example);
        }
        catch (ArgumentException ex)
        {
            return Failure(toolName, ex.Message, null, null);
        }
        catch (SqlException ex)
        {
            return Failure(
                toolName,
                $"SQL Server rejected the query: {ex.Message}",
                "The parameters were well-formed, so this came from the database itself: a name that "
                + "does not exist, a permission, or a WHERE clause it could not parse. Check the "
                + "object exists with list_tables or get_table_schema before querying it.",
                null);
        }
        catch (InvalidOperationException ex)
        {
            return Failure(toolName, ex.Message, null, null);
        }
    }

    private static string Failure(string toolName, string error, string? hint, object? example)
    {
        var payload = new Dictionary<string, object?>
        {
            ["ok"] = false,
            ["tool"] = toolName,
            ["error"] = error
        };

        if (!string.IsNullOrWhiteSpace(hint)) payload["hint"] = hint;
        if (example is not null) payload["example"] = example;

        return JsonSerializer.Serialize(payload);
    }
}
