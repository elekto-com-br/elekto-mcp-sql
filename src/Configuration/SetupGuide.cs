// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

namespace Elekto.Mcp.Sql.Configuration;

/// <summary>
/// What a caller needs in order to configure a connection: what is wrong, what to do about it,
/// where the file goes and what it looks like.
/// </summary>
/// <remarks>
/// The reader is usually a language model acting for a user who has just installed the server. It
/// can create the file itself once it knows the path and the shape, so both are stated outright
/// rather than left to the documentation.
/// </remarks>
internal sealed class SetupGuide
{
    private const string DocumentationUrl = "https://github.com/elekto-com-br/elekto-mcp-sql#configuration";

    /// <summary>What is wrong, in a sentence or two.</summary>
    public string Error { get; }

    /// <summary>What the caller should do about it.</summary>
    public string Hint { get; }

    /// <summary>The content of a valid connections file.</summary>
    public object Example { get; }

    /// <summary>Where the file goes, where the server looked, and the caveats.</summary>
    public IReadOnlyDictionary<string, object?> Setup { get; }

    /// <summary>The same guidance, as text the MCP client shows the model when it connects.</summary>
    public string Instructions { get; }

    private SetupGuide(string error, string hint, string file, IReadOnlyDictionary<string, object?> setup)
    {
        Error = error;
        Hint = hint;
        Example = ExampleFile;
        Setup = setup;
        Instructions =
            $"{error} Until that is fixed, every tool of this server answers with ok: false and the " +
            $"steps to fix it. Call list_databases for those steps, the file to create ({file}) and an " +
            "example of its content. The server looks again on every call, so once the file is in " +
            "place no restart is needed.";
    }

    /// <summary>
    /// Guidance for a server started without <c>--connections</c>, which reads every source
    /// <see cref="ConnectionConfig.TryDiscover"/> knows.
    /// </summary>
    /// <param name="problem">Why the sources could not be read, or null when none holds a connection.</param>
    public static SetupGuide ForDiscovery(string workingDirectory, string homeDirectory, string? problem)
    {
        var file = Path.Combine(workingDirectory, ConnectionConfig.LocalFileName);

        var (error, hint) = problem is null
            ? ("No database connection is configured: none of the places this server reads holds a " +
               "connection string.",
               "Tell the user, and agree with them before writing anything. With their go-ahead, create " +
               "the file named in 'setup.file' with the shape shown in 'example', for a login that can " +
               "only read. " + NoSecretsRule + " If the project already keeps its connection in a file " +
               "this server does not read, such as spring.datasource.url (jdbc:sqlserver://...) in " +
               "application.properties or application.yml, or a sqlserver:// or mssql:// URL in a .env " +
               "file, show the user where and build the entry from it, under the same rule. The server " +
               "looks again on every call, so the next one uses the file without a restart.")
            : ($"No database connection is configured: the configuration could not be read. {problem}",
               "Show the user the source the error names and, with their go-ahead, correct it; " +
               "'setup.searched' lists every place the server reads, and 'example' shows the shape of a " +
               "connections file. " + NoSecretsRule + " The server looks again on every call, so the " +
               "next one after the fix uses it without a restart.");

        return new SetupGuide(error, hint, file, new Dictionary<string, object?>
        {
            ["file"] = file,
            ["file_for_every_project"] = Path.Combine(homeDirectory, ConnectionConfig.LocalFileName),
            ["searched"] = ConnectionConfig.DescribeDiscoverySources(workingDirectory, homeDirectory),
            ["notes"] = new[]
            {
                CredentialsNote,
                ReadOnlyNote,
                "Instead of a file of its own, the server also takes the ConnectionStrings of the " +
                "project's appsettings.json, appsettings.Development.json, App.config or web.config, " +
                "and the MCP server command can name a file with --connections <path>."
            },
            ["documentation"] = DocumentationUrl
        });
    }

    /// <summary>Guidance for a server started with <c>--connections</c>, which reads that file alone.</summary>
    /// <param name="problem">Why the file could not be used.</param>
    public static SetupGuide ForConnectionsFile(string connectionsFile, string problem)
    {
        var file = Path.GetFullPath(connectionsFile);

        return new SetupGuide(
            $"No database connection is configured: the file given with --connections could not be used. {problem}",
            "Tell the user, and with their go-ahead create or correct the file named in 'setup.file', " +
            "with the shape shown in 'example'. " + NoSecretsRule + " While --connections is given no " +
            "other source is read. The server reads the file again on every " +
            "call, so the next one after the fix uses it without a restart.",
            file,
            new Dictionary<string, object?>
            {
                ["file"] = file,
                ["notes"] = new[] { CredentialsNote, ReadOnlyNote },
                ["documentation"] = DocumentationUrl
            });
    }

    // An agent following these steps writes files in the user's project; the rule keeps a password
    // out of every file it may write, and the decision to write anything with the user
    private const string NoSecretsRule =
        "Never write a password into it, nor anywhere else (a scratch file, a commit, a message): " +
        "put %{VARIABLE} in its place and ask the user to set that environment variable.";

    private const string CredentialsNote =
        "Keep passwords out of the connections file: write %{VARIABLE} in the connection string and " +
        "keep the secret in that environment variable, which the MCP client must pass to the server " +
        "(Codex passes only the variables listed in env_vars). Add the file to .gitignore all the same.";

    private const string ReadOnlyNote =
        "A login that can only read is enough for every tool. Once connected, check_permissions says " +
        "what the login can see and whether it could write.";

    // Both formats a database entry may take, and the %{VARIABLE} expansion, in one file
    private static readonly Dictionary<string, object> ExampleFile = new()
    {
        ["MyDatabase"] = "Server=SQLSRV01;Database=MyDatabase;Integrated Security=True;TrustServerCertificate=True",
        ["Reporting"] = new Dictionary<string, object>
        {
            ["connection_string"] =
                "Server=SQLSRV02;Database=Reporting;User Id=%{REPORTING_USER};Password=%{REPORTING_PASS};TrustServerCertificate=True",
            ["max_query_rows"] = 5000
        }
    };
}
