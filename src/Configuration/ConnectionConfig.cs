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
public sealed class ConnectionConfig
{
    /// <summary>Name of the environment variable used as fallback configuration source.</summary>
    public const string EnvVarName = "MCP_SQL_CONNECTIONS";

    /// <summary>Name of the local connections file searched in the working and home directories.</summary>
    public const string LocalFileName = ".elekto.mcp.conn.local.json";

    // Pattern to capture %{NAME} placeholders
    private static readonly Regex VarExpansionPattern = new(@"%\{([^}]+)\}", RegexOptions.Compiled);

    public IReadOnlyDictionary<string, DatabaseEntry> Databases { get; }

    private ConnectionConfig(IReadOnlyDictionary<string, DatabaseEntry> databases)
    {
        Databases = databases;
    }

    // -------------------------------------------------------------------------
    // Auto-discovery
    // -------------------------------------------------------------------------

    /// <summary>
    /// Resolves connections by walking the following chain and returning the first match:
    /// <list type="number">
    ///   <item><see cref="LocalFileName"/> in <paramref name="workingDirectory"/> (defaults to <see cref="Directory.GetCurrentDirectory"/>)</item>
    ///   <item><see cref="LocalFileName"/> in <paramref name="homeDirectory"/> (defaults to the user profile folder)</item>
    ///   <item><c>ConnectionStrings</c> section in <c>appsettings.Development.json</c> / <c>appsettings.json</c></item>
    ///   <item><c>&lt;connectionStrings&gt;</c> element in <c>web.config</c> / <c>App.config</c></item>
    ///   <item><see cref="EnvVarName"/> environment variable</item>
    /// </list>
    /// Returns both the resolved <see cref="ConnectionConfig"/> and a human-readable description of the source.
    /// Throws <see cref="InvalidOperationException"/> if nothing is found or JSON is invalid.
    /// Throws <see cref="ArgumentException"/> if a referenced environment variable does not exist.
    /// </summary>
    public static (ConnectionConfig Config, string Source) Discover(
        string? workingDirectory = null,
        string? homeDirectory = null)
    {
        workingDirectory ??= Directory.GetCurrentDirectory();
        homeDirectory ??= Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);

        // 1. Local file in the working directory (project root)
        var localFile = Path.Combine(workingDirectory, LocalFileName);
        if (File.Exists(localFile))
            return (LoadFromFile(localFile), localFile);

        // 2. Local file in the user home directory
        var homeFile = Path.Combine(homeDirectory, LocalFileName);
        if (File.Exists(homeFile))
            return (LoadFromFile(homeFile), homeFile);

        // 3. Connection strings from project config files
        var (fromProject, projectSource) = TryLoadFromProjectFiles(workingDirectory);
        if (fromProject is not null)
            return (fromProject, projectSource!);

        // 4. MCP_SQL_CONNECTIONS environment variable (backward compatibility)
        var raw = Environment.GetEnvironmentVariable(EnvVarName);
        if (raw is not null)
        {
            var source = $"environment variable '{EnvVarName}'";
            return (ParseJson(raw, source), source);
        }

        throw new InvalidOperationException(
            $"No connections found. Provide one of:\n" +
            $"  • {LocalFileName} in the project or home directory\n" +
            $"  • --connections <path>\n" +
            $"  • ConnectionStrings section in appsettings.json / web.config\n" +
            $"  • Environment variable '{EnvVarName}'");
    }

    // -------------------------------------------------------------------------
    // Explicit sources
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // Project file discovery
    // -------------------------------------------------------------------------

    /// <summary>
    /// Scans known project config files in <paramref name="directory"/> for connection strings.
    /// Processes files in precedence order: appsettings.json first, appsettings.Development.json
    /// last (so Development values override general ones, matching .NET behaviour).
    /// </summary>
    private static (ConnectionConfig? Config, string? Source) TryLoadFromProjectFiles(string directory)
    {
        var result = new Dictionary<string, DatabaseEntry>(StringComparer.OrdinalIgnoreCase);
        var sources = new List<string>();

        // JSON: appsettings.json then appsettings.Development.json (Development wins on conflict)
        foreach (var fileName in new[] { "appsettings.json", "appsettings.Development.json" })
        {
            var path = Path.Combine(directory, fileName);
            if (!File.Exists(path)) continue;
            try
            {
                var json = File.ReadAllText(path);
                using var doc = JsonDocument.Parse(json);
                if (!doc.RootElement.TryGetProperty("ConnectionStrings", out var cs) ||
                    cs.ValueKind != JsonValueKind.Object) continue;

                var added = false;
                foreach (var prop in cs.EnumerateObject())
                {
                    var connStr = prop.Value.GetString();
                    if (string.IsNullOrWhiteSpace(connStr)) continue;
                    result[prop.Name] = new DatabaseEntry
                    {
                        ConnectionString = ExpandVariables(connStr, prop.Name)
                    };
                    added = true;
                }
                if (added && !sources.Contains(path))
                    sources.Add(path);
            }
            catch (JsonException) { /* skip malformed */ }
            catch (IOException) { /* skip unreadable */ }
        }

        // XML: web.config, App.config
        foreach (var fileName in new[] { "web.config", "App.config" })
        {
            var path = Path.Combine(directory, fileName);
            if (!File.Exists(path)) continue;
            try
            {
                var doc = XDocument.Load(path);
                var adds = doc.Root?.Element("connectionStrings")?.Elements("add");
                if (adds is null) continue;

                var added = false;
                foreach (var el in adds)
                {
                    var name = el.Attribute("name")?.Value;
                    var connStr = el.Attribute("connectionString")?.Value;
                    if (string.IsNullOrWhiteSpace(name) || string.IsNullOrWhiteSpace(connStr)) continue;
                    result[name] = new DatabaseEntry
                    {
                        ConnectionString = ExpandVariables(connStr, name)
                    };
                    added = true;
                }
                if (added && !sources.Contains(path))
                    sources.Add(path);
            }
            catch (Exception ex) when (ex is System.Xml.XmlException or IOException) { /* skip malformed */ }
        }

        if (result.Count == 0)
            return (null, null);

        return (new ConnectionConfig(result), string.Join(" + ", sources.Select(Path.GetFileName)));
    }

    // -------------------------------------------------------------------------
    // Shared parsing
    // -------------------------------------------------------------------------

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
