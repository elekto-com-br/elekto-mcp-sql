using System.Text.Json;
using System.Text.RegularExpressions;

namespace Elekto.Mcp.Sql.Configuration;

/// <summary>
/// Representa a configuração de um banco de dados registrado.
/// </summary>
public sealed class DatabaseEntry
{
    public string ConnectionString { get; init; } = "";
    public int MaxQueryRows { get; init; } = 10_000;
}

/// <summary>
/// Carrega e valida a configuração de conexões a partir da variável de ambiente MCP_SQL_CONNECTIONS.
/// Suporta expansão de variáveis de ambiente com a sintaxe %{NOME_DA_VARIAVEL} dentro das strings de conexão.
/// </summary>
public sealed class ConnectionConfig
{
    public const string EnvVarName = "MCP_SQL_CONNECTIONS";

    // Expressão regular para capturar %{NOME}
    private static readonly Regex VarExpansionPattern = new(@"%\{([^}]+)\}", RegexOptions.Compiled);

    public IReadOnlyDictionary<string, DatabaseEntry> Databases { get; }

    private ConnectionConfig(IReadOnlyDictionary<string, DatabaseEntry> databases)
    {
        Databases = databases;
    }

    /// <summary>
    /// Carrega a configuração. Lança InvalidOperationException se a variável não estiver definida
    /// ou se o JSON for inválido. Lança ArgumentException se uma variável referenciada não existir.
    /// </summary>
    public static ConnectionConfig Load()
    {
        var raw = Environment.GetEnvironmentVariable(EnvVarName)
            ?? throw new InvalidOperationException(
                $"Variável de ambiente '{EnvVarName}' não definida. " +
                "Defina-a com um objeto JSON mapeando nomes para configurações de banco.");

        Dictionary<string, JsonElement> parsed;
        try
        {
            parsed = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(raw)
                ?? throw new InvalidOperationException("JSON vazio ou nulo.");
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"JSON inválido em '{EnvVarName}': {ex.Message}", ex);
        }

        var result = new Dictionary<string, DatabaseEntry>(StringComparer.OrdinalIgnoreCase);

        foreach (var (name, element) in parsed)
        {
            string rawConnStr;
            int maxRows = 10_000;

            // Aceita tanto string simples quanto objeto { connection_string, max_query_rows }
            if (element.ValueKind == JsonValueKind.String)
            {
                rawConnStr = element.GetString()!;
            }
            else if (element.ValueKind == JsonValueKind.Object)
            {
                rawConnStr = element.GetProperty("connection_string").GetString()
                    ?? throw new InvalidOperationException($"'{name}': 'connection_string' ausente ou nulo.");
                if (element.TryGetProperty("max_query_rows", out var maxEl))
                    maxRows = maxEl.GetInt32();
            }
            else
            {
                throw new InvalidOperationException($"'{name}': valor deve ser string ou objeto.");
            }

            var connStr = ExpandVariables(rawConnStr, name);
            result[name] = new DatabaseEntry { ConnectionString = connStr, MaxQueryRows = maxRows };
        }

        return new ConnectionConfig(result);
    }

    private static string ExpandVariables(string input, string dbName)
    {
        return VarExpansionPattern.Replace(input, match =>
        {
            var varName = match.Groups[1].Value;
            return Environment.GetEnvironmentVariable(varName)
                ?? throw new ArgumentException(
                    $"Banco '{dbName}': variável de ambiente '%{{{varName}}}' não encontrada.");
        });
    }
}
