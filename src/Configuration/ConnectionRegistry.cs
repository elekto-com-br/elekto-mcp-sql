// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

namespace Elekto.Mcp.Sql.Configuration;

/// <summary>
/// The connections the tools work with or, while there are none, why not and how to add one.
/// </summary>
/// <remarks>
/// <para>
/// A server that exits at startup for want of a connection is shown by the MCP client only as
/// "failed", and the reason, written to stderr, ends up in a log nobody opens. Registered once for
/// every project (as <c>claude mcp add -s user</c> does), it would fail in each project without a
/// connection. So the server starts regardless, and every tool answers with what is missing and
/// how to provide it: a caller can act on that, while a process that exited tells it nothing.
/// </para>
/// <para>
/// While nothing is loaded, every call looks again. A connections file created after the server
/// started, by the user or by the agent on the user's behalf, is used by the very next call with no
/// restart. Once connections are loaded they are kept for the life of the process, as before.
/// </para>
/// </remarks>
public sealed class ConnectionRegistry
{
    private readonly string? _connectionsFile;
    private readonly string _workingDirectory;
    private readonly string _homeDirectory;
    private readonly Lock _gate = new();

    private ConnectionConfig? _config;
    private SetupGuide? _problem;

    /// <param name="connectionsFile">The file given with <c>--connections</c>, or null to read every source.</param>
    /// <param name="workingDirectory">Where project files are looked for; the current directory by default.</param>
    /// <param name="homeDirectory">Where the user's own file is looked for; the profile directory by default.</param>
    public ConnectionRegistry(string? connectionsFile, string? workingDirectory = null, string? homeDirectory = null)
    {
        _connectionsFile = connectionsFile;
        _workingDirectory = workingDirectory ?? Directory.GetCurrentDirectory();
        _homeDirectory = homeDirectory ?? Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);

        lock (_gate) TryLoad();
    }

    /// <summary>Where the loaded connections came from; null while none are loaded.</summary>
    public string? Source { get; private set; }

    /// <summary>Why no connection is loaded and how to fix it; null once connections are loaded.</summary>
    internal SetupGuide? Problem
    {
        get { lock (_gate) return _problem; }
    }

    /// <summary>
    /// The loaded connections. While there are none, looks again, and throws
    /// <see cref="NotConfiguredException"/> if it still finds none.
    /// </summary>
    public ConnectionConfig GetConfig()
    {
        lock (_gate)
        {
            if (_config is null) TryLoad();
            return _config ?? throw new NotConfiguredException(_problem!);
        }
    }

    private void TryLoad()
    {
        try
        {
            if (_connectionsFile is not null)
            {
                var config = ConnectionConfig.LoadFromFile(_connectionsFile);
                if (config.Databases.Count == 0)
                {
                    _problem = SetupGuide.ForConnectionsFile(_connectionsFile, "The file holds no connection.");
                    return;
                }
                Accept(config, _connectionsFile);
            }
            else if (ConnectionConfig.TryDiscover(_workingDirectory, _homeDirectory) is (var found, var source))
            {
                Accept(found, source);
            }
            else
            {
                _problem = SetupGuide.ForDiscovery(_workingDirectory, _homeDirectory, problem: null);
            }
        }
        catch (Exception ex)
        {
            // Any failure to read the configuration leaves the server unconfigured, not stopped
            _problem = _connectionsFile is not null
                ? SetupGuide.ForConnectionsFile(_connectionsFile, ex.Message)
                : SetupGuide.ForDiscovery(_workingDirectory, _homeDirectory, ex.Message);
        }
    }

    private void Accept(ConnectionConfig config, string source)
    {
        _config = config;
        Source = source;
        _problem = null;
    }
}
