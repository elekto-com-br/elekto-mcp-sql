using System.Text.Json;
using Elekto.Mcp.Sql.Configuration;
using Elekto.Mcp.Sql.Tools;

namespace Elekto.Mcp.Sql.Tests;

/// <summary>
/// Sem conexões o servidor sobe mesmo assim, e cada ferramenta explica como configurar uma.
/// </summary>
[TestFixture]
public class ConnectionRegistryTests
{
    private string _dir = "";
    private string _home = "";

    [SetUp]
    public void Setup()
    {
        // Discover() também lê a variável de ambiente; garante que ela não interfira
        Environment.SetEnvironmentVariable(ConnectionConfig.EnvVarName, null);
        _dir = MakeTempDir();
        _home = MakeTempDir();
    }

    [TearDown]
    public void Cleanup()
    {
        Environment.SetEnvironmentVariable(ConnectionConfig.EnvVarName, null);
        DeleteDir(_dir);
        DeleteDir(_home);
    }

    // -------------------------------------------------------------------------
    // Descoberta automática (sem --connections)
    // -------------------------------------------------------------------------

    [Test]
    public void NothingFound_DoesNotThrow_AndRecordsWhy()
    {
        var registry = new ConnectionRegistry(null, _dir, _home);

        Assert.That(registry.Problem, Is.Not.Null);
        Assert.That(registry.Source, Is.Null);
        Assert.That(registry.Problem!.Error, Does.Contain("none of the places"));
    }

    [Test]
    public void NothingFound_GetConfigThrows_WithTheFileToCreate()
    {
        var registry = new ConnectionRegistry(null, _dir, _home);

        var ex = Assert.Throws<NotConfiguredException>(() => registry.GetConfig());

        Assert.That(ex!.Guide.Setup["file"], Is.EqualTo(Path.Combine(_dir, ConnectionConfig.LocalFileName)));
        Assert.That(ex.Guide.Setup["file_for_every_project"],
            Is.EqualTo(Path.Combine(_home, ConnectionConfig.LocalFileName)));
        Assert.That((IReadOnlyList<string>)ex.Guide.Setup["searched"]!,
            Has.Member(Path.Combine(_dir, ConnectionConfig.LocalFileName)));
    }

    [Test]
    public void FileCreatedAfterStart_IsUsedByTheNextCall()
    {
        var registry = new ConnectionRegistry(null, _dir, _home);
        Assert.That(registry.Problem, Is.Not.Null);

        WriteFile(_dir, ConnectionConfig.LocalFileName,
            """{"Risk": "Server=.;Database=Risk;Integrated Security=SSPI"}""");

        var config = registry.GetConfig();

        Assert.That(config.Databases, Contains.Key("Risk"));
        Assert.That(registry.Problem, Is.Null);
        Assert.That(registry.Source, Does.Contain(ConnectionConfig.LocalFileName));
    }

    [Test]
    public void FoundAtStart_IsLoaded()
    {
        WriteFile(_dir, "appsettings.json",
            """{"ConnectionStrings": {"Risk": "Server=.;Database=Risk;Integrated Security=SSPI"}}""");

        var registry = new ConnectionRegistry(null, _dir, _home);

        Assert.That(registry.Problem, Is.Null);
        Assert.That(registry.GetConfig().Databases, Contains.Key("Risk"));
        Assert.That(registry.Source, Is.EqualTo("appsettings.json"));
    }

    [Test]
    public void UnreadableSource_IsReported_NotThrown()
    {
        WriteFile(_dir, ConnectionConfig.LocalFileName, "{ this is not json");

        var registry = new ConnectionRegistry(null, _dir, _home);

        Assert.That(registry.Problem, Is.Not.Null);
        Assert.That(registry.Problem!.Error, Does.Contain("could not be read"));
        Assert.That(registry.Problem.Error, Does.Contain("Invalid JSON"));
    }

    [Test]
    public void MissingVariable_IsReported_NotThrown()
    {
        WriteFile(_dir, ConnectionConfig.LocalFileName,
            """{"Risk": "Server=.;Database=Risk;User Id=%{ELEKTO_TEST_UNDEFINED_VAR};Password=x"}""");

        var registry = new ConnectionRegistry(null, _dir, _home);

        Assert.That(registry.Problem, Is.Not.Null);
        Assert.That(registry.Problem!.Error, Does.Contain("ELEKTO_TEST_UNDEFINED_VAR"));
    }

    // -------------------------------------------------------------------------
    // Arquivo explícito (--connections)
    // -------------------------------------------------------------------------

    [Test]
    public void ExplicitFile_Missing_IsReported_AndUsedOnceCreated()
    {
        var path = Path.Combine(_dir, "conns.json");
        var registry = new ConnectionRegistry(path, _dir, _home);

        var ex = Assert.Throws<NotConfiguredException>(() => registry.GetConfig());
        Assert.That(ex!.Guide.Error, Does.Contain("--connections"));
        Assert.That(ex.Guide.Setup["file"], Is.EqualTo(Path.GetFullPath(path)));
        Assert.That(ex.Guide.Setup.ContainsKey("searched"), Is.False);

        File.WriteAllText(path, """{"Risk": "Server=.;Database=Risk;Integrated Security=SSPI"}""");

        Assert.That(registry.GetConfig().Databases, Contains.Key("Risk"));
        Assert.That(registry.Source, Is.EqualTo(path));
    }

    [Test]
    public void ExplicitFile_Empty_IsNotConfigured()
    {
        var path = Path.Combine(_dir, "conns.json");
        File.WriteAllText(path, "{}");

        var registry = new ConnectionRegistry(path, _dir, _home);

        Assert.That(registry.Problem, Is.Not.Null);
        Assert.That(registry.Problem!.Error, Does.Contain("holds no connection"));
    }

    [Test]
    public void ExplicitFile_IgnoresOtherSources()
    {
        // Com --connections nenhuma outra fonte é lida, nem quando o arquivo falta
        WriteFile(_dir, ConnectionConfig.LocalFileName,
            """{"Other": "Server=.;Database=Other;Integrated Security=SSPI"}""");

        var registry = new ConnectionRegistry(Path.Combine(_dir, "conns.json"), _dir, _home);

        Assert.That(registry.Problem, Is.Not.Null);
    }

    // -------------------------------------------------------------------------
    // O que as ferramentas devolvem
    // -------------------------------------------------------------------------

    [Test]
    public void ListDatabases_WithoutConnections_ExplainsHowToConfigure()
    {
        var tools = new SqlTools(new ConnectionRegistry(null, _dir, _home));

        using var doc = JsonDocument.Parse(tools.list_databases());
        var root = doc.RootElement;

        Assert.That(root.GetProperty("ok").GetBoolean(), Is.False);
        Assert.That(root.GetProperty("tool").GetString(), Is.EqualTo("list_databases"));
        Assert.That(root.GetProperty("hint").GetString(), Is.Not.Empty);
        Assert.That(root.GetProperty("example").TryGetProperty("MyDatabase", out _), Is.True);
        Assert.That(root.GetProperty("setup").GetProperty("file").GetString(),
            Is.EqualTo(Path.Combine(_dir, ConnectionConfig.LocalFileName)));
    }

    [Test]
    public void AnyTool_WithoutConnections_ExplainsHowToConfigure()
    {
        var tools = new SqlTools(new ConnectionRegistry(null, _dir, _home));

        using var doc = JsonDocument.Parse(tools.query_table("Risk", "Trades"));
        var root = doc.RootElement;

        Assert.That(root.GetProperty("ok").GetBoolean(), Is.False);
        Assert.That(root.GetProperty("tool").GetString(), Is.EqualTo("query_table"));
        Assert.That(root.TryGetProperty("setup", out _), Is.True);
    }

    [Test]
    public void ListDatabases_WithConnections_KeepsItsShape()
    {
        WriteFile(_dir, ConnectionConfig.LocalFileName,
            """{"Risk": {"connection_string": "Server=.;Database=Risk;Integrated Security=SSPI", "max_query_rows": 500}}""");
        var tools = new SqlTools(new ConnectionRegistry(null, _dir, _home));

        using var doc = JsonDocument.Parse(tools.list_databases());
        var root = doc.RootElement;

        Assert.That(root.ValueKind, Is.EqualTo(JsonValueKind.Array));
        Assert.That(root[0].GetProperty("name").GetString(), Is.EqualTo("Risk"));
        Assert.That(root[0].GetProperty("max_query_rows").GetInt32(), Is.EqualTo(500));
    }

    [Test]
    public void Guidance_AsksTheUserFirst_AndKeepsPasswordsOutOfFiles()
    {
        // O agente que segue estas instruções escreve no projeto do usuário: precisa combinar antes
        // e nunca gravar uma senha
        var discovery = new ConnectionRegistry(null, _dir, _home).Problem!;
        var explicitFile = new ConnectionRegistry(Path.Combine(_dir, "conns.json"), _dir, _home).Problem!;

        WriteFile(_dir, ConnectionConfig.LocalFileName, "{ not json");
        var unreadable = new ConnectionRegistry(null, _dir, _home).Problem!;

        foreach (var hint in new[] { discovery.Hint, explicitFile.Hint, unreadable.Hint })
            Assert.That(hint, Does.Contain("go-ahead").And.Contain("Never write a password").And.Contain("%{VARIABLE}"));

        Assert.That(discovery.Hint, Does.Contain("application.properties").And.Contain(".env"));
    }

    [Test]
    public void Instructions_NameTheFileToCreate()
    {
        var registry = new ConnectionRegistry(null, _dir, _home);

        Assert.That(registry.Problem!.Instructions,
            Does.Contain(Path.Combine(_dir, ConnectionConfig.LocalFileName)));
        Assert.That(registry.Problem.Instructions, Does.Contain("list_databases"));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

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
