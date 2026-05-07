// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

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
    public int DefaultTimeoutSeconds { get; init; } = 30;
}

/// <summary>
/// Loads and validates connection configuration from a JSON file (via <c>--connections</c> argument)
/// or from the <c>MCP_SQL_CONNECTIONS</c> environment variable as fallback.
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
    /// Loads configuration from a JSON file at the given path.
    /// Throws <see cref="InvalidOperationException"/> if the file does not exist or the JSON is invalid.
    /// Throws <see cref="ArgumentException"/> if a referenced environment variable does not exist.
    /// </summary>
    public static ConnectionConfig LoadFromFile(string filePath)
    {
        if (!File.Exists(filePath))
            throw new InvalidOperationException(
                $"Connections file not found: '{filePath}'.");

        string json;
        try
        {
            json = File.ReadAllText(filePath);
        }
        catch (IOException ex)
        {
            throw new InvalidOperationException(
                $"Failed to read connections file '{filePath}': {ex.Message}", ex);
        }

        return ParseJson(json, $"file '{filePath}'");
    }

    /// <summary>
    /// Loads configuration from the <c>MCP_SQL_CONNECTIONS</c> environment variable.
    /// Throws <see cref="InvalidOperationException"/> if the variable is not defined or the JSON is invalid.
    /// Throws <see cref="ArgumentException"/> if a referenced environment variable does not exist.
    /// </summary>
    public static ConnectionConfig Load()
    {
        var raw = Environment.GetEnvironmentVariable(EnvVarName)
            ?? throw new InvalidOperationException(
                $"Environment variable '{EnvVarName}' is not defined and no --connections file was specified. " +
                "Pass --connections <path> or set the environment variable to a JSON object.");

        return ParseJson(raw, $"environment variable '{EnvVarName}'");
    }

    private static ConnectionConfig ParseJson(string json, string source)
    {
        Dictionary<string, JsonElement> parsed;
        try
        {
            parsed = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(json)
                ?? throw new InvalidOperationException("Empty or null JSON.");
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Invalid JSON in {source}: {ex.Message}", ex);
        }

        var result = new Dictionary<string, DatabaseEntry>(StringComparer.OrdinalIgnoreCase);

        foreach (var (name, element) in parsed)
        {
            string rawConnStr;
            int maxRows = 10_000;
            int defaultTimeoutSeconds = 30;

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
                if (element.TryGetProperty("default_timeout_seconds", out var timeoutEl))
                    defaultTimeoutSeconds = timeoutEl.GetInt32();
            }
            else
            {
                throw new InvalidOperationException($"'{name}': value must be a string or an object.");
            }

            if (maxRows <= 0)
                throw new InvalidOperationException($"'{name}': 'max_query_rows' must be greater than zero.");

            if (defaultTimeoutSeconds <= 0)
                throw new InvalidOperationException($"'{name}': 'default_timeout_seconds' must be greater than zero.");

            var connStr = ExpandVariables(rawConnStr, name);
            result[name] = new DatabaseEntry
            {
                ConnectionString = connStr,
                MaxQueryRows = maxRows,
                DefaultTimeoutSeconds = defaultTimeoutSeconds
            };
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
