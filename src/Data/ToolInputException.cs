// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

namespace Elekto.Mcp.Sql.Data;

/// <summary>
/// An input error that carries enough for the caller to fix the call and retry, rather than only
/// stating that something was wrong.
/// </summary>
/// <remarks>
/// The caller is usually a language model, which cannot inspect this server's source and only sees
/// what comes back. "Invalid value" tells it a call failed; it does not tell it what to send instead,
/// so it guesses again. A hint and a worked example turn one failure into one correction.
/// </remarks>
public sealed class ToolInputException : Exception
{
    /// <summary>What the caller should do differently, in a sentence.</summary>
    public string? Hint { get; }

    /// <summary>A minimal, correct call fragment for the parameter that failed.</summary>
    public object? Example { get; }

    public ToolInputException(string message, string? hint = null, object? example = null)
        : base(message)
    {
        Hint = hint;
        Example = example;
    }

    public ToolInputException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
