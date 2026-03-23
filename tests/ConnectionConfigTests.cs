using Elekto.Mcp.Sql.Configuration;

namespace Elekto.Mcp.Sql.Tests;

[TestFixture]
public class ConnectionConfigTests
{
    private const string EnvVar = ConnectionConfig.EnvVarName;

    [TearDown]
    public void Cleanup() => Environment.SetEnvironmentVariable(EnvVar, null);

    // -------------------------------------------------------------------------
    // Formato simples (string de conexão direta)
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
    // Formato objeto
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
    // Múltiplos bancos
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
    // Expansão de variáveis de ambiente
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
        // Garante que a variável não existe
        Environment.SetEnvironmentVariable("TEST_MCP_NONEXISTENT", null);
        Environment.SetEnvironmentVariable(EnvVar,
            """{"MyDb": "Server=.;Password=%{TEST_MCP_NONEXISTENT}"}""");

        Assert.Throws<ArgumentException>(() => ConnectionConfig.Load());
    }

    // -------------------------------------------------------------------------
    // Erros de configuração
    // -------------------------------------------------------------------------

    [Test]
    public void Load_EnvVarNotSet_ThrowsInvalidOperationException()
    {
        // EnvVar já foi limpa no TearDown do teste anterior (ou nunca foi setada)
        Assert.Throws<InvalidOperationException>(() => ConnectionConfig.Load());
    }

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
}
