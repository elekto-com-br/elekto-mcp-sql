// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

using Elekto.Mcp.Sql.Tests.Infrastructure;

namespace Elekto.Mcp.Sql.Tests;

/// <summary>
/// Stops the SQL Server container, if <see cref="TestServer"/> started one, once every test in the
/// assembly has run. The server itself is resolved lazily, so runs with only unit tests start nothing.
/// </summary>
[SetUpFixture]
public class TestServerLifetime
{
    [OneTimeTearDown]
    public static Task StopServer() => TestServer.ReleaseAsync();
}
