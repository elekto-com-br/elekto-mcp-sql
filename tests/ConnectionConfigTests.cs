using Elekto.Mcp.Sql.Configuration;

namespace Elekto.Mcp.Sql.Tests;

[TestFixture]
public class ConnectionConfigTests
{
    private const string EnvVar = ConnectionConfig.EnvVarName;

    [TearDown]
    public void Cleanup() => Environment.SetEnvironmentVariable(EnvVar, null);

    // -------------------------------------------------------------------------
    // Simple format (a bare connection string), through the environment variable
    // -------------------------------------------------------------------------

    [Test]
    public void Load_SimpleString_ParsesConnectionString()
    {
        Environment.SetEnvironmentVariable(EnvVar,
            """{"MyDb": "Server=.;Database=Test;Integrated Security=SSPI"}""");

        var config = ConnectionConfig.Load();

        Assert.That(config.Databases, Contains.Key("MyDb"));
        Assert.That(config.Databases["MyDb"].ConnectionString,
            Is.EqualTo("Server=.;Database=Test;Integrated Security=SSPI"));
    }

    [Test]
    public void Load_SimpleString_UsesDefaultMaxRows()
    {
        Environment.SetEnvironmentVariable(EnvVar,
            """{"MyDb": "Server=.;Database=Test;Integrated Security=SSPI"}""");

        var config = ConnectionConfig.Load();

        Assert.That(config.Databases["MyDb"].MaxQueryRows, Is.EqualTo(10_000));
    }

    // -------------------------------------------------------------------------
    // Object format
    // -------------------------------------------------------------------------

    [Test]
    public void Load_ObjectFormat_ParsesAllFields()
    {
        Environment.SetEnvironmentVariable(EnvVar, """
            {
              "MyDb": {
                "connection_string": "Server=.;Database=Test;Integrated Security=SSPI",
                "max_query_rows": 500,
                "default_timeout_seconds": 45
              }
            }
            """);

        var config = ConnectionConfig.Load();

        Assert.That(config.Databases["MyDb"].ConnectionString,
            Is.EqualTo("Server=.;Database=Test;Integrated Security=SSPI"));
        Assert.That(config.Databases["MyDb"].MaxQueryRows, Is.EqualTo(500));
        Assert.That(config.Databases["MyDb"].DefaultTimeoutSeconds, Is.EqualTo(45));
    }

    [Test]
    public void Load_ObjectFormat_UsesDefaultTimeout_WhenMissing()
    {
        Environment.SetEnvironmentVariable(EnvVar, """
            {
              "MyDb": {
                "connection_string": "Server=.;Database=Test;Integrated Security=SSPI",
                "max_query_rows": 500
              }
            }
            """);

        var config = ConnectionConfig.Load();

        Assert.That(config.Databases["MyDb"].DefaultTimeoutSeconds, Is.EqualTo(30));
    }

    [Test]
    public void Load_InvalidDefaultTimeout_ThrowsInvalidOperationException()
    {
        Environment.SetEnvironmentVariable(EnvVar, """
            {
              "MyDb": {
                "connection_string": "Server=.;Database=Test;Integrated Security=SSPI",
                "default_timeout_seconds": 0
              }
            }
            """);

        Assert.Throws<InvalidOperationException>(() => ConnectionConfig.Load());
    }

    // -------------------------------------------------------------------------
    // Several databases
    // -------------------------------------------------------------------------

    [Test]
    public void Load_MultipleDatabases_RegistersAll()
    {
        Environment.SetEnvironmentVariable(EnvVar, """
            {
              "Alpha": "Server=.;Database=A;Integrated Security=SSPI",
              "Beta":  "Server=.;Database=B;Integrated Security=SSPI"
            }
            """);

        var config = ConnectionConfig.Load();

        Assert.That(config.Databases.Keys, Is.EquivalentTo(new[] { "Alpha", "Beta" }));
    }

    // -------------------------------------------------------------------------
    // Environment variable expansion
    // -------------------------------------------------------------------------

    [Test]
    public void Load_VariableExpansion_ReplacesPlaceholder()
    {
        Environment.SetEnvironmentVariable("TEST_MCP_USER", "sa");
        Environment.SetEnvironmentVariable("TEST_MCP_PASS", "secret");
        Environment.SetEnvironmentVariable(EnvVar,
            """{"MyDb": "Server=.;User Id=%{TEST_MCP_USER};Password=%{TEST_MCP_PASS}"}""");

        try
        {
            var config = ConnectionConfig.Load();
            Assert.That(config.Databases["MyDb"].ConnectionString,
                Is.EqualTo("Server=.;User Id=sa;Password=secret"));
        }
        finally
        {
            Environment.SetEnvironmentVariable("TEST_MCP_USER", null);
            Environment.SetEnvironmentVariable("TEST_MCP_PASS", null);
        }
    }

    [Test]
    public void Load_MissingVariable_ThrowsArgumentException()
    {
        // Make sure the variable does not exist
        Environment.SetEnvironmentVariable("TEST_MCP_NONEXISTENT", null);
        Environment.SetEnvironmentVariable(EnvVar,
            """{"MyDb": "Server=.;Password=%{TEST_MCP_NONEXISTENT}"}""");

        Assert.Throws<ArgumentException>(() => ConnectionConfig.Load());
    }

    // -------------------------------------------------------------------------
    // Configuration errors, environment variable
    // -------------------------------------------------------------------------

    [Test]
    public void Load_EnvVarNotSet_ThrowsInvalidOperationException() =>
        // EnvVar was cleared by the previous test's TearDown (or never set)
        Assert.Throws<InvalidOperationException>(() => ConnectionConfig.Load());

    [Test]
    public void Load_InvalidJson_ThrowsInvalidOperationException()
    {
        Environment.SetEnvironmentVariable(EnvVar, "not json at all");

        Assert.Throws<InvalidOperationException>(() => ConnectionConfig.Load());
    }

    [Test]
    public void Load_LookupIsCaseInsensitive()
    {
        Environment.SetEnvironmentVariable(EnvVar,
            """{"RiskSystem": "Server=.;Database=Risk;Integrated Security=SSPI"}""");

        var config = ConnectionConfig.Load();

        Assert.That(config.Databases.ContainsKey("risksystem"), Is.True);
        Assert.That(config.Databases.ContainsKey("RISKSYSTEM"), Is.True);
    }

    // -------------------------------------------------------------------------
    // LoadFromFile, loading from a file
    // -------------------------------------------------------------------------

    [Test]
    public void LoadFromFile_SimpleString_ParsesConnectionString()
    {
        var path = WriteTempFile("""{"MyDb": "Server=.;Database=Test;Integrated Security=SSPI"}""");
        try
        {
            var config = ConnectionConfig.LoadFromFile(path);

            Assert.That(config.Databases, Contains.Key("MyDb"));
            Assert.That(config.Databases["MyDb"].ConnectionString,
                Is.EqualTo("Server=.;Database=Test;Integrated Security=SSPI"));
        }
        finally { File.Delete(path); }
    }

    [Test]
    public void LoadFromFile_ObjectFormat_ParsesAllFields()
    {
        var path = WriteTempFile("""
            {
              "MyDb": {
                "connection_string": "Server=.;Database=Test;Integrated Security=SSPI",
                "max_query_rows": 250,
                "default_timeout_seconds": 90
              }
            }
            """);
        try
        {
            var config = ConnectionConfig.LoadFromFile(path);

            Assert.That(config.Databases["MyDb"].MaxQueryRows, Is.EqualTo(250));
            Assert.That(config.Databases["MyDb"].DefaultTimeoutSeconds, Is.EqualTo(90));
        }
        finally { File.Delete(path); }
    }

    [Test]
    public void LoadFromFile_MultipleDatabases_RegistersAll()
    {
        var path = WriteTempFile("""
            {
              "Alpha": "Server=.;Database=A;Integrated Security=SSPI",
              "Beta":  "Server=.;Database=B;Integrated Security=SSPI"
            }
            """);
        try
        {
            var config = ConnectionConfig.LoadFromFile(path);

            Assert.That(config.Databases.Keys, Is.EquivalentTo(new[] { "Alpha", "Beta" }));
        }
        finally { File.Delete(path); }
    }

    [Test]
    public void LoadFromFile_VariableExpansion_ReplacesPlaceholder()
    {
        Environment.SetEnvironmentVariable("TEST_MCP_FILE_PASS", "p4ss");
        var path = WriteTempFile(
            """{"MyDb": "Server=.;Password=%{TEST_MCP_FILE_PASS}"}""");
        try
        {
            var config = ConnectionConfig.LoadFromFile(path);

            Assert.That(config.Databases["MyDb"].ConnectionString,
                Is.EqualTo("Server=.;Password=p4ss"));
        }
        finally
        {
            File.Delete(path);
            Environment.SetEnvironmentVariable("TEST_MCP_FILE_PASS", null);
        }
    }

    [Test]
    public void LoadFromFile_FileMissing_ThrowsInvalidOperationException() => Assert.Throws<InvalidOperationException>(() =>
                                                                                       ConnectionConfig.LoadFromFile("nonexistent_connections_file_xyz.json"));

    [Test]
    public void LoadFromFile_InvalidJson_ThrowsInvalidOperationException()
    {
        var path = WriteTempFile("not json at all");
        try
        {
            Assert.Throws<InvalidOperationException>(() => ConnectionConfig.LoadFromFile(path));
        }
        finally { File.Delete(path); }
    }

    [Test]
    public void LoadFromFile_LookupIsCaseInsensitive()
    {
        var path = WriteTempFile(
            """{"RiskSystem": "Server=.;Database=Risk;Integrated Security=SSPI"}""");
        try
        {
            var config = ConnectionConfig.LoadFromFile(path);

            Assert.That(config.Databases.ContainsKey("risksystem"), Is.True);
            Assert.That(config.Databases.ContainsKey("RISKSYSTEM"), Is.True);
        }
        finally { File.Delete(path); }
    }

    // -------------------------------------------------------------------------
    // Discover(), merging every source by priority
    // -------------------------------------------------------------------------

    [Test]
    public void Discover_MergesBothLocalFiles_UniqueNames()
    {
        // Different names in home and project: both appear in the result
        var dir = MakeTempDir();
        var homedir = MakeTempDir();
        try
        {
            WriteFile(homedir, ConnectionConfig.LocalFileName,
                """{"HomeDb": "Server=HOME;Database=H;Integrated Security=SSPI"}""");
            WriteFile(dir, ConnectionConfig.LocalFileName,
                """{"WorkDb": "Server=PROJ;Database=W;Integrated Security=SSPI"}""");

            var (config, source) = ConnectionConfig.Discover(dir, homedir);

            Assert.That(config.Databases, Contains.Key("HomeDb"));
            Assert.That(config.Databases, Contains.Key("WorkDb"));
            Assert.That(source, Does.Contain("(~)"));
            Assert.That(source, Does.Contain("(project)"));
        }
        finally { DeleteDir(dir); DeleteDir(homedir); }
    }

    [Test]
    public void Discover_ProjectLocalFile_OverridesHomeFile_SameName()
    {
        // Same name in both sources: the project wins
        var dir = MakeTempDir();
        var homedir = MakeTempDir();
        try
        {
            WriteFile(homedir, ConnectionConfig.LocalFileName,
                """{"SharedDb": "Server=HOME;Database=Shared;Integrated Security=SSPI"}""");
            WriteFile(dir, ConnectionConfig.LocalFileName,
                """{"SharedDb": "Server=PROJECT;Database=Shared;Integrated Security=SSPI"}""");

            var (config, _) = ConnectionConfig.Discover(dir, homedir);

            Assert.That(config.Databases["SharedDb"].ConnectionString,
                Does.Contain("Server=PROJECT"));
        }
        finally { DeleteDir(dir); DeleteDir(homedir); }
    }

    [Test]
    public void Discover_LocalFileInHomeDir_ContributesWhenNoProjectFile()
    {
        var dir = MakeTempDir();
        var homedir = MakeTempDir();
        try
        {
            WriteFile(homedir, ConnectionConfig.LocalFileName,
                """{"HomeDb": "Server=.;Database=Home;Integrated Security=SSPI"}""");

            var (config, _) = ConnectionConfig.Discover(dir, homedir);

            Assert.That(config.Databases, Contains.Key("HomeDb"));
        }
        finally { DeleteDir(dir); DeleteDir(homedir); }
    }

    [Test]
    public void Discover_AppSettingsJson_ParsesConnectionStrings()
    {
        var dir = MakeTempDir();
        var homedir = MakeTempDir();
        try
        {
            WriteFile(dir, "appsettings.json", """
                {
                  "ConnectionStrings": {
                    "AppDb": "Server=.;Database=App;Integrated Security=SSPI"
                  }
                }
                """);

            var (config, source) = ConnectionConfig.Discover(dir, homedir);

            Assert.That(config.Databases, Contains.Key("AppDb"));
            Assert.That(source, Does.Contain("appsettings.json"));
        }
        finally { DeleteDir(dir); DeleteDir(homedir); }
    }

    [Test]
    public void Discover_AppSettingsDevelopmentJson_OverridesAppSettings()
    {
        // Development overrides appsettings.json for the same name
        var dir = MakeTempDir();
        var homedir = MakeTempDir();
        try
        {
            WriteFile(dir, "appsettings.json", """
                {
                  "ConnectionStrings": {
                    "AppDb": "Server=PROD;Database=App;Integrated Security=SSPI"
                  }
                }
                """);
            WriteFile(dir, "appsettings.Development.json", """
                {
                  "ConnectionStrings": {
                    "AppDb": "Server=DEV;Database=App;Integrated Security=SSPI"
                  }
                }
                """);

            var (config, _) = ConnectionConfig.Discover(dir, homedir);

            Assert.That(config.Databases["AppDb"].ConnectionString,
                Does.Contain("Server=DEV"));
        }
        finally { DeleteDir(dir); DeleteDir(homedir); }
    }

    [Test]
    public void Discover_ProjectLocalFile_OverridesAppSettings_SameName()
    {
        // The project's .elekto.mcp.sql.local.json takes priority over appsettings
        var dir = MakeTempDir();
        var homedir = MakeTempDir();
        try
        {
            WriteFile(dir, "appsettings.json", """
                {
                  "ConnectionStrings": {
                    "SharedDb": "Server=APPSETTINGS;Database=D;Integrated Security=SSPI"
                  }
                }
                """);
            WriteFile(dir, ConnectionConfig.LocalFileName,
                """{"SharedDb": "Server=LOCAL;Database=D;Integrated Security=SSPI"}""");

            var (config, _) = ConnectionConfig.Discover(dir, homedir);

            Assert.That(config.Databases["SharedDb"].ConnectionString,
                Does.Contain("Server=LOCAL"));
        }
        finally { DeleteDir(dir); DeleteDir(homedir); }
    }

    [Test]
    public void Discover_AllSourcesMerged_UniqueNamesAccumulate()
    {
        // Each source contributes a different name: all of them appear
        var dir = MakeTempDir();
        var homedir = MakeTempDir();
        Environment.SetEnvironmentVariable(EnvVar,
            """{"EnvDb": "Server=.;Database=Env;Integrated Security=SSPI"}""");
        try
        {
            WriteFile(homedir, ConnectionConfig.LocalFileName,
                """{"HomeDb": "Server=.;Database=Home;Integrated Security=SSPI"}""");
            WriteFile(dir, "appsettings.json", """
                {
                  "ConnectionStrings": {
                    "AppDb": "Server=.;Database=App;Integrated Security=SSPI"
                  }
                }
                """);
            WriteFile(dir, ConnectionConfig.LocalFileName,
                """{"WorkDb": "Server=.;Database=Work;Integrated Security=SSPI"}""");

            var (config, source) = ConnectionConfig.Discover(dir, homedir);

            Assert.That(config.Databases, Contains.Key("EnvDb"));
            Assert.That(config.Databases, Contains.Key("HomeDb"));
            Assert.That(config.Databases, Contains.Key("AppDb"));
            Assert.That(config.Databases, Contains.Key("WorkDb"));
            // Source must list every source that contributed
            Assert.That(source, Does.Contain(EnvVar));
            Assert.That(source, Does.Contain("appsettings.json"));
        }
        finally
        {
            DeleteDir(dir);
            DeleteDir(homedir);
        }
    }

    [Test]
    public void Discover_WebConfig_ParsesConnectionStrings()
    {
        var dir = MakeTempDir();
        var homedir = MakeTempDir();
        try
        {
            WriteFile(dir, "web.config", """
                <?xml version="1.0" encoding="utf-8"?>
                <configuration>
                  <connectionStrings>
                    <add name="LegacyDb"
                         connectionString="Server=.;Database=Legacy;Integrated Security=SSPI"
                         providerName="System.Data.SqlClient" />
                  </connectionStrings>
                </configuration>
                """);

            var (config, source) = ConnectionConfig.Discover(dir, homedir);

            Assert.That(config.Databases, Contains.Key("LegacyDb"));
            Assert.That(source, Does.Contain("web.config"));
        }
        finally { DeleteDir(dir); DeleteDir(homedir); }
    }

    [Test]
    public void Discover_AppConfig_ParsesConnectionStrings()
    {
        var dir = MakeTempDir();
        var homedir = MakeTempDir();
        try
        {
            WriteFile(dir, "App.config", """
                <?xml version="1.0" encoding="utf-8"?>
                <configuration>
                  <connectionStrings>
                    <add name="DesktopDb"
                         connectionString="Server=.;Database=Desktop;Integrated Security=SSPI" />
                  </connectionStrings>
                </configuration>
                """);

            var (config, _) = ConnectionConfig.Discover(dir, homedir);

            Assert.That(config.Databases, Contains.Key("DesktopDb"));
        }
        finally { DeleteDir(dir); DeleteDir(homedir); }
    }

    [Test]
    public void Discover_EnvVar_ContributesWhenNoFilesFound()
    {
        var dir = MakeTempDir();
        var homedir = MakeTempDir();
        Environment.SetEnvironmentVariable(EnvVar,
            """{"EnvDb": "Server=.;Database=Env;Integrated Security=SSPI"}""");
        try
        {
            var (config, source) = ConnectionConfig.Discover(dir, homedir);

            Assert.That(config.Databases, Contains.Key("EnvDb"));
            Assert.That(source, Does.Contain(EnvVar));
        }
        finally { DeleteDir(dir); DeleteDir(homedir); }
    }

    [Test]
    public void Discover_ThrowsInvalidOperationException_WhenNothingFound()
    {
        var dir = MakeTempDir();
        var homedir = MakeTempDir();
        Environment.SetEnvironmentVariable(EnvVar, null);
        try
        {
            Assert.Throws<InvalidOperationException>(() => ConnectionConfig.Discover(dir, homedir));
        }
        finally { DeleteDir(dir); DeleteDir(homedir); }
    }

    [Test]
    public void Discover_AppSettingsMergesMultipleDatabases()
    {
        var dir = MakeTempDir();
        var homedir = MakeTempDir();
        try
        {
            WriteFile(dir, "appsettings.json", """
                {
                  "ConnectionStrings": {
                    "Alpha": "Server=.;Database=A;Integrated Security=SSPI",
                    "Beta":  "Server=.;Database=B;Integrated Security=SSPI"
                  }
                }
                """);

            var (config, _) = ConnectionConfig.Discover(dir, homedir);

            Assert.That(config.Databases.Keys, Is.EquivalentTo(new[] { "Alpha", "Beta" }));
        }
        finally { DeleteDir(dir); DeleteDir(homedir); }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static string WriteTempFile(string content)
    {
        var path = Path.Combine(Path.GetTempPath(), $"mcp_test_{Guid.NewGuid():N}.json");
        File.WriteAllText(path, content);
        return path;
    }

    private static string MakeTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"mcp_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static void WriteFile(string dir, string name, string content)
        => File.WriteAllText(Path.Combine(dir, name), content);

    private static void DeleteDir(string dir)
    {
        if (Directory.Exists(dir))
            Directory.Delete(dir, recursive: true);
    }
}
