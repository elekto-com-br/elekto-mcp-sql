// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using Elekto.Mcp.Sql.Data;
using Elekto.Mcp.Sql.Tools;

namespace Elekto.Mcp.Sql.Tests;

/// <summary>
/// What the tools answer when no connection to SQL Server can be made. These need no server: only a
/// port on this machine where nothing listens, or where something listens and never answers.
/// </summary>
[TestFixture]
public class ConnectionFailureTests
{
    [Test]
    public void NothingListening_IsAFailedConnection()
    {
        var reader = new SchemaReader($"Data Source=127.0.0.1,{FreePort()};Connect Timeout=3");

        var ex = Assert.Throws<ConnectionFailedException>(() => reader.ListSchemas());

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.StartWith("Could not connect to SQL Server (error "));
            Assert.That(ex.Hint, Is.EqualTo(SqlErrorHint.Connection));
        });
    }

    [Test]
    public void ServerThatNeverAnswers_IsAFailedConnection_NotAQueryTimeout()
    {
        // The listener takes the TCP connection but never answers SQL Server's handshake, so the
        // client gives up with -2, the same number a query that runs too long gets.
        using var silent = new SilentServer();
        var reader = new SchemaReader(silent.ConnectionString);

        var ex = Assert.Throws<ConnectionFailedException>(() => reader.ListSchemas());

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.StartWith("Could not connect to SQL Server (error "));
            Assert.That(ex.Hint, Is.EqualTo(SqlErrorHint.Connection));
        });
    }

    [Test]
    public void CallsAtOnceToAServerThatNeverAnswers_AllSayTheConnectionFailed()
    {
        // The pool makes a call wait for another one's attempt to connect and, when the time runs
        // out, throws an InvalidOperationException that blames a full pool.
        using var silent = new SilentServer();

        var calls = Enumerable.Range(0, 3)
            .Select(_ => Task.Run(() => ToolResponse.Guard("list_schemas",
                () => new SchemaReader(silent.ConnectionString).ListSchemas())))
            .ToArray();
        Task.WaitAll(calls);

        Assert.Multiple(() =>
        {
            foreach (var root in calls.Select(c => JsonSerializer.Deserialize<JsonElement>(c.Result)))
            {
                Assert.That(root.GetProperty("error").GetString(), Does.StartWith(SqlErrorHint.CouldNotConnect));
                Assert.That(root.GetProperty("hint").GetString(), Is.EqualTo(SqlErrorHint.Connection));
            }
        });
    }

    [Test]
    public void Tool_AnswersOkFalse_WithoutBlamingTheQuery()
    {
        var reader = new SchemaReader($"Data Source=127.0.0.1,{FreePort()};Connect Timeout=3");

        var root = JsonSerializer.Deserialize<JsonElement>(
            ToolResponse.Guard("list_schemas", () => reader.ListSchemas()));

        Assert.Multiple(() =>
        {
            Assert.That(root.GetProperty("ok").GetBoolean(), Is.False);
            Assert.That(root.GetProperty("error").GetString(),
                Does.StartWith("Could not connect to SQL Server (error ").And.Not.Contain("rejected the query"));
            Assert.That(root.GetProperty("hint").GetString(), Is.EqualTo(SqlErrorHint.Connection));
        });
    }

    /// <summary>A port on this machine where nothing listens: one the system has just handed out and taken back.</summary>
    private static int FreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    /// <summary>
    /// Takes TCP connections and never answers them. The connection string is its own, so each test
    /// gets a pool of its own.
    /// </summary>
    private sealed class SilentServer : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);

        public SilentServer() => _listener.Start();

        public string ConnectionString =>
            $"Data Source=127.0.0.1,{((IPEndPoint)_listener.LocalEndpoint).Port};Connect Timeout=2";

        public void Dispose() => _listener.Stop();
    }
}
