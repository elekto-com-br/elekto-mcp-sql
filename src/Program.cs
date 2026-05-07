// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

using Elekto.Mcp.Sql.Configuration;
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

// Loads and validates configuration before starting the host.
// Priority: --connections <path> > .elekto.mcp.conn.local.json (project) >
//           .elekto.mcp.conn.local.json (~) > appsettings.json > web.config > MCP_SQL_CONNECTIONS
ConnectionConfig config;
try
{
    var connectionsFile = ParseConnectionsArg(args);
    string source;

    if (connectionsFile is not null)
    {
        config = ConnectionConfig.LoadFromFile(connectionsFile);
        source = connectionsFile;
    }
    else
    {
        (config, source) = ConnectionConfig.Discover();
    }

    await Console.Error.WriteLineAsync(
        $"[Elekto.Mcp.Sql] {config.Databases.Count} connection(s) loaded from {source}");
}
catch (Exception ex)
{
    await Console.Error.WriteLineAsync($"[Elekto.Mcp.Sql] Configuration error: {ex.Message}");
    return 1;
}

// Registers config as a singleton for injection into tools
builder.Services.AddSingleton(config);

// Registers the MCP server with stdio transport
builder.Services
    .AddMcpServer()
    .WithStdioServerTransport()
    .WithToolsFromAssembly();

await builder.Build().RunAsync();
return 0;

// Scans args for --connections <path> and returns the path, or null if not present.
static string? ParseConnectionsArg(string[] args)
{
    for (var i = 0; i < args.Length - 1; i++)
        if (args[i] is "--connections" or "-c")
            return args[i + 1];
    return null;
}
