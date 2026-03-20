using System.Text.Json;
using System.Text.RegularExpressions;

namespace Elekto.Mcp.Sql.Configuration;

/// <summary>
/// Represents the configuration of a registered database.
/// </summary>
public sealed class DatabaseEntry
{
    public string ConnectionString { get; init; } = "";
    public int MaxQueryRows { get; init; } = 10_000;
}

/// <summary>
/// Loads and validates connection configuration from the <c>MCP_SQL_CONNECTIONS</c> environment variable.
/// Supports environment variable expansion using <c>%{VARIABLE_NAME}</c> syntax inside connection strings.
/// </summary>
public sealed class ConnectionConfig
{
    public const string EnvVarName = "MCP_SQL_CONNECTIONS";

    // Pattern to capture %{NAME} placeholders
    private static readonly Regex VarExpansionPattern = new(@"%\{([^}]+)\}", RegexOptions.Compiled);

    public IReadOnlyDictionary<string, DatabaseEntry> Databases { get; }

    private ConnectionConfig(IReadOnlyDictionary<string, DatabaseEntry> databases)
    {
        Databases = databases;
    }

    /// <summary>
    /// Loads the configuration. Throws <see cref="InvalidOperationException"/> if the variable is
    /// not defined or the JSON is invalid. Throws <see cref="ArgumentException"/> if a referenced
    /// environment variable does not exist.
    /// </summary>
    public static ConnectionConfig Load()
    {
        var raw = Environment.GetEnvironmentVariable(EnvVarName)
            ?? throw new InvalidOperationException(
                $"Environment variable '{EnvVarName}' is not defined. " +
                "Set it to a JSON object mapping database names to their configurations.");

        Dictionary<string, JsonElement> parsed;
        try
        {
            parsed = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(raw)
                ?? throw new InvalidOperationException("Empty or null JSON.");
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Invalid JSON in '{EnvVarName}': {ex.Message}", ex);
        }

        var result = new Dictionary<string, DatabaseEntry>(StringComparer.OrdinalIgnoreCase);

        foreach (var (name, element) in parsed)
        {
            string rawConnStr;
            int maxRows = 10_000;

            // Accepts both a plain string and an object { connection_string, max_query_rows }
            if (element.ValueKind == JsonValueKind.String)
            {
                rawConnStr = element.GetString()!;
            }
            else if (element.ValueKind == JsonValueKind.Object)
            {
                rawConnStr = element.GetProperty("connection_string").GetString()
                    ?? throw new InvalidOperationException($"'{name}': 'connection_string' is missing or null.");
                if (element.TryGetProperty("max_query_rows", out var maxEl))
                    maxRows = maxEl.GetInt32();
            }
            else
            {
                throw new InvalidOperationException($"'{name}': value must be a string or an object.");
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
                    $"Database '{dbName}': environment variable '%{{{varName}}}' not found.");
        });
    }
}
