using Elekto.Mcp.Sql.Configuration;
using Elekto.Mcp.Sql.Tools;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// Redireciona logs para stderr para não poluir o canal stdio do MCP
var builder = Host.CreateApplicationBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddConsole(options =>
{
    options.LogToStandardErrorThreshold = LogLevel.Trace;
});

// Carrega e valida configuração antes de subir o host
ConnectionConfig config;
try
{
    config = ConnectionConfig.Load();
}
catch (Exception ex)
{
    await Console.Error.WriteLineAsync($"[Elekto.Mcp.Sql] Erro de configuração: {ex.Message}");
    return 1;
}

// Registra a config como singleton para injeção nas tools
builder.Services.AddSingleton(config);

// Registra o servidor MCP com transporte stdio
builder.Services
    .AddMcpServer()
    .WithStdioServerTransport()
    .WithToolsFromAssembly();

await builder.Build().RunAsync();
return 0;
