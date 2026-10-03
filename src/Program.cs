// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

using Elekto.Mcp.Sql.Configuration;
using Elekto.Mcp.Sql.Tools;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// Redirects logs to stderr to avoid polluting the MCP stdio channel
var builder = Host.CreateApplicationBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddConsole(options =>
{
    options.LogToStandardErrorThreshold = LogLevel.Trace;
});

// Loads the connections before starting the host.
// Priority: --connections <path> > .elekto.mcp.sql.local.json (project) > appsettings.Development.json >
//           appsettings.json > App.config / web.config > .elekto.mcp.sql.local.json (~) > MCP_SQL_CONNECTIONS
// A configuration that cannot be loaded does not stop the server: every tool then explains how to
// configure one (see ConnectionRegistry for why).
var connections = new ConnectionRegistry(ParseConnectionsArg(args));
var problem = connections.Problem;

if (problem is null)
    await Console.Error.WriteLineAsync(
        $"[Elekto.Mcp.Sql] {connections.GetConfig().Databases.Count} connection(s) loaded from {connections.Source}");
else
    await Console.Error.WriteLineAsync(
        $"[Elekto.Mcp.Sql] {problem.Error} Starting anyway; the tools will say how to configure a connection.");

// Registers the connections as a singleton for injection into tools
builder.Services.AddSingleton(connections);

// Registers the MCP server with stdio transport
builder.Services
    .AddMcpServer(options =>
    {
        // How the tools fit together; without connections the problem comes first, so the client
        // learns it on connecting, before the model calls any tool
        options.ServerInstructions = ServerInstructions.For(problem?.Instructions);
    })
    .WithStdioServerTransport()
    .WithToolsFromAssembly();

await builder.Build().RunAsync();

// Scans args for --connections <path> and returns the path, or null if not present.
static string? ParseConnectionsArg(string[] args)
{
    for (var i = 0; i < args.Length - 1; i++)
        if (args[i] is "--connections" or "-c")
            return args[i + 1];
    return null;
}
