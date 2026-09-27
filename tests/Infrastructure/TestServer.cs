// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

using System.Text;
using Microsoft.Data.SqlClient;
using Testcontainers.MsSql;

namespace Elekto.Mcp.Sql.Tests.Infrastructure;

/// <summary>
/// Finds a SQL Server for the integration tests, trying in order:
/// <list type="number">
///   <item>the connection string in <see cref="ConnectionStringVariable"/>;</item>
///   <item>LocalDB, when running on Windows and the instance answers;</item>
///   <item>a SQL Server container started through Testcontainers (needs Docker);</item>
/// </list>
/// and failing, with what was tried, only when none of them works.
/// The server is resolved once per test run; a container, if started, is stopped by <see cref="TestServerLifetime"/>.
/// </summary>
public static class TestServer
{
    /// <summary>
    /// Connection string to a SQL Server the tests may use. The ElektoMcpTest database is dropped and
    /// recreated there, so the login needs dbcreator rights; any database named in it is ignored.
    /// </summary>
    public const string ConnectionStringVariable = "ELEKTO_MCP_SQL_CONN_TEST";

    public const string LocalDbInstance = @"(localdb)\MSSQLLocalDB";

    public const string ContainerImage = "mcr.microsoft.com/mssql/server:2022-latest";

    private static readonly SemaphoreSlim Gate = new(1, 1);
    private static string? _masterConnectionString;
    private static MsSqlContainer? _container;

    /// <summary>Human readable description of where the server came from, for the test log.</summary>
    public static string Source { get; private set; } = "(not resolved)";

    /// <summary>Returns a connection string pointing to the master database of the resolved server.</summary>
    public static async Task<string> GetMasterConnectionStringAsync()
    {
        if (_masterConnectionString != null) return _masterConnectionString;

        await Gate.WaitAsync();
        try
        {
            return _masterConnectionString ??= await ResolveAsync();
        }
        finally
        {
            Gate.Release();
        }
    }

    private static async Task<string> ResolveAsync()
    {
        var attempts = new StringBuilder();

        // 1) Explicit connection string
        var fromEnvironment = Environment.GetEnvironmentVariable(ConnectionStringVariable);
        if (!string.IsNullOrWhiteSpace(fromEnvironment))
        {
            var conn = WithMaster(fromEnvironment);
            // An explicit choice that does not work is an error, not a reason to silently use something else.
            await ProbeAsync(conn);
            return Resolved(conn, $"environment variable {ConnectionStringVariable}");
        }
        attempts.AppendLine($"- {ConnectionStringVariable}: not set");

        // 2) LocalDB
        if (OperatingSystem.IsWindows())
        {
            var conn = WithMaster($"Server={LocalDbInstance};Integrated Security=SSPI;TrustServerCertificate=True;Connect Timeout=30");
            try
            {
                await ProbeAsync(conn);
                return Resolved(conn, $"LocalDB {LocalDbInstance}");
            }
            catch (Exception ex)
            {
                attempts.AppendLine($"- LocalDB: {ex.Message}");
            }
        }
        else
        {
            attempts.AppendLine("- LocalDB: not on Windows");
        }

        // 3) Container
        try
        {
            var container = new MsSqlBuilder(ContainerImage).Build();
            try
            {
                await container.StartAsync();
            }
            catch
            {
                await container.DisposeAsync();
                throw;
            }
            _container = container;
            return Resolved(WithMaster(container.GetConnectionString()), $"container {ContainerImage}");
        }
        catch (Exception ex)
        {
            attempts.AppendLine($"- Testcontainers ({ContainerImage}): {ex.Message}");
        }

        // 4) Give up
        throw new InvalidOperationException(
            "No SQL Server available for the integration tests. Tried:" + Environment.NewLine + attempts +
            $"Set {ConnectionStringVariable}, install LocalDB (Windows) or make Docker available.");
    }

    private static string Resolved(string connectionString, string source)
    {
        Source = source;
        TestContext.Progress.WriteLine($"Integration tests using SQL Server from {source}.");
        return connectionString;
    }

    private static string WithMaster(string connectionString) =>
        new SqlConnectionStringBuilder(connectionString) { InitialCatalog = "master" }.ConnectionString;

    private static async Task ProbeAsync(string connectionString)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
    }

    /// <summary>Stops the container, if one was started.</summary>
    internal static async Task ReleaseAsync()
    {
        if (_container == null) return;
        await _container.DisposeAsync();
        _container = null;
        _masterConnectionString = null;
    }
}
