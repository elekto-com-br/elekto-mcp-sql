// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

using System.Text.Json;
using System.Text.RegularExpressions;
using System.Xml.Linq;

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
/// Loads and validates connection configuration. Supports multiple sources via <see cref="Discover"/>.
/// Supports environment variable expansion using <c>%{VARIABLE_NAME}</c> syntax inside connection strings.
/// </summary>
public sealed partial class ConnectionConfig
{
    /// <summary>Name of the environment variable used as fallback configuration source.</summary>
    public const string EnvVarName = "MCP_SQL_CONNECTIONS";

    /// <summary>Name of the local connections file searched in the working and home directories.</summary>
    public const string LocalFileName = ".elekto.mcp.sql.local.json";

    // Pattern to capture %{NAME} placeholders
    private static readonly Regex VarExpansionPattern = EnvironmentVariableRegex();

    public IReadOnlyDictionary<string, DatabaseEntry> Databases { get; }

    private ConnectionConfig(IReadOnlyDictionary<string, DatabaseEntry> databases) => Databases = databases;

    // -------------------------------------------------------------------------
    // Auto-discovery (merge)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Resolves connections by merging all available sources in priority order.
    /// When the same database name appears in multiple sources, the higher-priority source wins.
    /// Sources, from lowest to highest priority:
    /// <list type="number">
    ///   <item><see cref="EnvVarName"/> environment variable</item>
    ///   <item><see cref="LocalFileName"/> in <paramref name="homeDirectory"/> (~)</item>
    ///   <item><c>web.config</c> / <c>App.config</c> in <paramref name="workingDirectory"/></item>
    ///   <item><c>appsettings.json</c> in <paramref name="workingDirectory"/></item>
    ///   <item><c>appsettings.Development.json</c> in <paramref name="workingDirectory"/></item>
    ///   <item><see cref="LocalFileName"/> in <paramref name="workingDirectory"/> (project — highest)</item>
    /// </list>
    /// Returns both the merged <see cref="ConnectionConfig"/> and a human-readable description of all
    /// contributing sources. Throws <see cref="InvalidOperationException"/> if no connections are found.
    /// </summary>
    public static (ConnectionConfig Config, string Source) Discover(
        string? workingDirectory = null,
        string? homeDirectory = null)
    {
        workingDirectory ??= Directory.GetCurrentDirectory();
        homeDirectory ??= Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);

        var merged = new Dictionary<string, DatabaseEntry>(StringComparer.OrdinalIgnoreCase);
        var sources = new List<string>();

        // Merges dict into merged, overwriting existing keys (higher priority called last)
        void Absorb(Dictionary<string, DatabaseEntry>? dict, string label)
        {
            if (dict is null || dict.Count == 0) return;
            foreach (var (k, v) in dict) merged[k] = v;
            sources.Add(label);
        }

        // ---- Lowest priority first ----

        // 1. MCP_SQL_CONNECTIONS env var
        Absorb(TryLoadDictFromEnvVar(), $"env:{EnvVarName}");

        // 2. ~/.elekto.mcp.sql.local.json  (global user defaults)
        Absorb(TryLoadDictFromLocalFile(Path.Combine(homeDirectory, LocalFileName)),
               $"{LocalFileName} (~)");

        // 3. web.config / App.config  (project XML, both may contribute)
        foreach (var xmlFile in new[] { "App.config", "web.config" })
            Absorb(TryLoadDictFromXmlConfig(Path.Combine(workingDirectory, xmlFile)), xmlFile);

        // 4. appsettings.json → appsettings.Development.json  (Development overrides general)
        foreach (var jsonFile in new[] { "appsettings.json", "appsettings.Development.json" })
            Absorb(TryLoadDictFromAppSettings(Path.Combine(workingDirectory, jsonFile)), jsonFile);

        // 5. ./.elekto.mcp.sql.local.json  (project-level — highest priority)
        Absorb(TryLoadDictFromLocalFile(Path.Combine(workingDirectory, LocalFileName)),
               $"{LocalFileName} (project)");

        // ---- End of chain ----

        if (merged.Count == 0)
            throw new InvalidOperationException(
                $"No connections found. Provide one of:\n" +
                $"  • {LocalFileName} in the project or home directory\n" +
                $"  • --connections <path>\n" +
                $"  • ConnectionStrings section in appsettings.json / web.config\n" +
                $"  • Environment variable '{EnvVarName}'");

        return (new ConnectionConfig(merged), string.Join(" + ", sources));
    }

    // -------------------------------------------------------------------------
    // Explicit sources (single-source, no merge)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Loads configuration exclusively from a JSON file at the given path.
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

        return new ConnectionConfig(ParseJsonToDict(json, $"file '{filePath}'"));
    }

    /// <summary>
    /// Loads configuration exclusively from the <c>MCP_SQL_CONNECTIONS</c> environment variable.
    /// Throws <see cref="InvalidOperationException"/> if the variable is not defined or the JSON is invalid.
    /// Throws <see cref="ArgumentException"/> if a referenced environment variable does not exist.
    /// </summary>
    public static ConnectionConfig Load()
    {
        var raw = Environment.GetEnvironmentVariable(EnvVarName)
            ?? throw new InvalidOperationException(
                $"Environment variable '{EnvVarName}' is not defined and no --connections file was specified. " +
                "Pass --connections <path> or set the environment variable to a JSON object.");

        return new ConnectionConfig(ParseJsonToDict(raw, $"environment variable '{EnvVarName}'"));
    }

    // -------------------------------------------------------------------------
    // Private loaders — each returns null when the source is absent/empty
    // -------------------------------------------------------------------------

    private static Dictionary<string, DatabaseEntry>? TryLoadDictFromEnvVar()
    {
        var raw = Environment.GetEnvironmentVariable(EnvVarName);
        if (raw is null) return null;
        try { return ParseJsonToDict(raw, $"env:{EnvVarName}"); }
        catch (JsonException) { return null; }
    }

    private static Dictionary<string, DatabaseEntry>? TryLoadDictFromLocalFile(string path)
    {
        if (!File.Exists(path)) return null;
        try
        {
            var json = File.ReadAllText(path);
            return ParseJsonToDict(json, $"file '{path}'");
        }
        catch (IOException) { return null; }
    }

    private static Dictionary<string, DatabaseEntry>? TryLoadDictFromAppSettings(string path)
    {
        if (!File.Exists(path)) return null;
        try
        {
            var json = File.ReadAllText(path);
            using var doc = JsonDocument.Parse(json);
            if (!doc.RootElement.TryGetProperty("ConnectionStrings", out var cs) ||
                cs.ValueKind != JsonValueKind.Object) return null;

            var result = new Dictionary<string, DatabaseEntry>(StringComparer.OrdinalIgnoreCase);
            foreach (var prop in cs.EnumerateObject())
            {
                var connStr = prop.Value.GetString();
                if (!string.IsNullOrWhiteSpace(connStr))
                    result[prop.Name] = new DatabaseEntry
                    {
                        ConnectionString = ExpandVariables(connStr, prop.Name)
                    };
            }
            return result.Count > 0 ? result : null;
        }
        catch (Exception ex) when (ex is JsonException or IOException) { return null; }
    }

    private static Dictionary<string, DatabaseEntry>? TryLoadDictFromXmlConfig(string path)
    {
        if (!File.Exists(path)) return null;
        try
        {
            var doc = XDocument.Load(path);
            var adds = doc.Root?.Element("connectionStrings")?.Elements("add");
            if (adds is null) return null;

            var result = new Dictionary<string, DatabaseEntry>(StringComparer.OrdinalIgnoreCase);
            foreach (var el in adds)
            {
                var name = el.Attribute("name")?.Value;
                var connStr = el.Attribute("connectionString")?.Value;
                if (!string.IsNullOrWhiteSpace(name) && !string.IsNullOrWhiteSpace(connStr))
                    result[name] = new DatabaseEntry
                    {
                        ConnectionString = ExpandVariables(connStr, name)
                    };
            }
            return result.Count > 0 ? result : null;
        }
        catch (Exception ex) when (ex is System.Xml.XmlException or IOException) { return null; }
    }

    // -------------------------------------------------------------------------
    // Shared JSON parser
    // -------------------------------------------------------------------------

    private static Dictionary<string, DatabaseEntry> ParseJsonToDict(string json, string source)
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

            result[name] = new DatabaseEntry
            {
                ConnectionString = ExpandVariables(rawConnStr, name),
                MaxQueryRows = maxRows,
                DefaultTimeoutSeconds = defaultTimeoutSeconds
            };
        }

        return result;
    }

    private static string ExpandVariables(string input, string dbName) 
        => VarExpansionPattern.Replace(input, 
            match =>
            {
                var varName = match.Groups[1].Value;
                return Environment.GetEnvironmentVariable(varName)
                    ?? throw new ArgumentException(
                        $"Database '{dbName}': environment variable '%{{{varName}}}' not found.");
            });

    /// <summary>
    /// Regex pattern for environment variable expansion: %{VARNAME}
    /// Enforces POSIX-compliant variable names: [A-Za-z_][A-Za-z0-9_]*
    /// Valid: DB_USER, _PRIVATE, db_user, DB123
    /// Invalid: 123DB (leading digit), DB-PASS (hyphen), DB PASS (space)
    /// </summary>
    [GeneratedRegex(@"%\{([A-Za-z_][A-Za-z0-9_]*)\}", RegexOptions.Compiled)]
    private static partial Regex EnvironmentVariableRegex();
}
