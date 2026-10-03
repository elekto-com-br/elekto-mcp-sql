// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

namespace Elekto.Mcp.Sql.Configuration;

/// <summary>
/// Raised when a tool is called and no connection is configured. Carries the steps to configure
/// one, which the tool returns in place of a result.
/// </summary>
public sealed class NotConfiguredException : Exception
{
    internal SetupGuide Guide { get; }

    internal NotConfiguredException(SetupGuide guide) : base(guide.Error) => Guide = guide;
}
