// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

using System.Text.Json;
using Elekto.Mcp.Sql.Data;
using Elekto.Mcp.Sql.Tests.Infrastructure;

namespace Elekto.Mcp.Sql.Tests;

/// <summary>
/// The same reader, run as logins that cannot see everything. SQL Server filters the catalog for
/// them without an error, so what is tested here is that the server says so instead of answering
/// as though it saw the whole database.
/// </summary>
[TestFixture]
public class RestrictedLoginSchemaReaderTests
{
    private static TestDatabase _db = null!;

    [OneTimeSetUp]
    public static async Task CreateDatabase() => _db = await TestDatabase.CreateAsync();

    [OneTimeTearDown]
    public static void DropDatabase() => _db?.Dispose();

    private static SchemaReader Owner => new(_db.ConnectionString);
    private static SchemaReader Reader => _db.ReaderAs(TestDatabase.ReaderLogin);
    private static SchemaReader Viewer => _db.ReaderAs(TestDatabase.ViewerLogin);
    private static SchemaReader Inserter => _db.ReaderAs(TestDatabase.InsertLogin);
    private static SchemaReader Narrow => _db.ReaderAs(TestDatabase.NarrowLogin);

    private static JsonElement Parse(string json) => JsonSerializer.Deserialize<JsonElement>(json);

    private static JsonElement Area(JsonElement report, string area) =>
        report.GetProperty("tools").EnumerateArray().Single(t => t.GetProperty("area").GetString() == area);

    #region The owner sees everything

    [Test]
    public void Owner_Overview_ReportsCompleteVisibility()
    {
        var overview = Parse(Owner.GetDatabaseOverview());

        var visibility = overview.GetProperty("visibility");
        Assert.That(visibility.GetProperty("complete").GetBoolean(), Is.True);
        Assert.That(visibility.TryGetProperty("notes", out _), Is.False);
    }

    [Test]
    public void Owner_CheckPermissions_EveryAreaIsComplete_AndNothingToGrant()
    {
        var report = Parse(Owner.CheckPermissions());

        Assert.That(report.GetProperty("tools").EnumerateArray().Select(t => t.GetProperty("status").GetString()),
            Is.All.EqualTo("complete"));
        Assert.That(report.GetProperty("grant_script").ValueKind, Is.EqualTo(JsonValueKind.Null));
    }

    [Test]
    public void Owner_CheckPermissions_IsNotReadOnly()
    {
        var writeAccess = Parse(Owner.CheckPermissions()).GetProperty("write_access");

        Assert.That(writeAccess.GetProperty("read_only").GetBoolean(), Is.False);
        Assert.That(writeAccess.GetProperty("roles").EnumerateArray().Select(r => r.GetString()),
            Has.Member("db_owner"));
    }

    #endregion

    #region db_datareader alone

    [Test]
    public void Reader_Overview_CountsOnlyWhatItSees_AndSaysSo()
    {
        var overview = Parse(Reader.GetDatabaseOverview());

        // The seed has two procedures; this login holds no permission on either, so SQL Server
        // reports none — which is exactly the answer that must not pass for the truth.
        Assert.That(overview.GetProperty("procedure_count").GetInt32(), Is.Zero);
        var visibility = overview.GetProperty("visibility");
        Assert.That(visibility.GetProperty("complete").GetBoolean(), Is.False);
        Assert.That(visibility.GetProperty("notes").EnumerateArray().Select(n => n.GetString()),
            Has.Some.Contains("VIEW DEFINITION"));
        Assert.That(visibility.GetProperty("hint").GetString(), Does.Contain("check_permissions"));
    }

    [Test]
    public void Reader_Overview_StillSeesEveryTable()
    {
        var overview = Parse(Reader.GetDatabaseOverview());

        Assert.That(overview.GetProperty("table_count").GetInt32(), Is.EqualTo(3));
    }

    [Test]
    public void Reader_ListProcedures_DoesNotFail()
    {
        var rows = Parse(Reader.ListProcedures(schema: null));

        Assert.That(rows.ValueKind, Is.EqualTo(JsonValueKind.Array));
        Assert.That(rows.GetArrayLength(), Is.Zero);
    }

    [Test]
    public void Reader_ListFunctions_HiddenDefinitionsHaveNoMetrics()
    {
        // SELECT on the database reaches the two table-valued functions, but not the scalar one,
        // and none of their text.
        var rows = Parse(Reader.ListFunctions(schema: null)).EnumerateArray().ToList();

        Assert.That(rows.Select(r => r.GetProperty("function_name").GetString()),
            Is.EquivalentTo(new[] { "fn_InstrumentosVencendoEm", "fn_PosicoesAcimaDe" }));
        foreach (var row in rows)
        {
            Assert.That(row.GetProperty("definition_visible").GetBoolean(), Is.False);
            Assert.That(row.GetProperty("line_count").ValueKind, Is.EqualTo(JsonValueKind.Null),
                "A hidden definition must not read as a body of zero lines.");
        }
    }

    [Test]
    public void Reader_GetProcedureDefinition_OfAnInvisibleProcedure_SaysItMayBeHidden()
    {
        var ex = Assert.Throws<ToolInputException>(() => Reader.GetProcedureDefinition("sp_ObterInstrumento", schema: null));

        Assert.That(ex!.Message, Does.Contain("visible to this login"));
        Assert.That(ex.Hint, Does.Contain("VIEW DEFINITION"));
    }

    [Test]
    public void Reader_GetViewDefinition_SeesTheViewButNotItsText()
    {
        var result = Parse(Reader.GetViewDefinition("vw_InstrumentosAtivos", schema: "dbo"));

        var definition = result.GetProperty("definition")[0];
        Assert.That(definition.GetProperty("definition_visible").GetBoolean(), Is.False);
        Assert.That(definition.GetProperty("definition").ValueKind, Is.EqualTo(JsonValueKind.Null));
        Assert.That(definition.GetProperty("definition_hint").GetString(), Does.Contain("VIEW DEFINITION"));
        Assert.That(result.GetProperty("columns").GetArrayLength(), Is.GreaterThan(0));
    }

    [Test]
    public void Reader_GetIndexHealth_KeepsTheDuplicates_AndMarksTheDmvSectionsUnavailable()
    {
        var result = Parse(Reader.GetIndexHealth(schema: "financeiro"));

        Assert.That(result.GetProperty("duplicate_indexes").GetArrayLength(), Is.GreaterThan(0));
        Assert.That(result.GetProperty("unused_indexes").ValueKind, Is.EqualTo(JsonValueKind.Null));
        Assert.That(result.GetProperty("missing_index_suggestions").ValueKind, Is.EqualTo(JsonValueKind.Null));

        var visibility = result.GetProperty("visibility");
        Assert.That(visibility.GetProperty("complete").GetBoolean(), Is.False);
        Assert.That(visibility.GetProperty("unavailable").EnumerateArray()
                .Select(u => u.GetProperty("section").GetString()),
            Is.EquivalentTo(new[] { "unused_indexes", "missing_index_suggestions" }));
    }

    [Test]
    public void Reader_GetTableUsage_StillReportsForeignKeys_AndSaysModulesMayBeMissing()
    {
        var result = Parse(Reader.GetTableUsage("Instrumento", schema: "dbo"));

        Assert.That(result.GetProperty("foreign_key_usage").GetArrayLength(), Is.GreaterThan(0));
        Assert.That(result.GetProperty("visibility").GetProperty("complete").GetBoolean(), Is.False);
    }

    [Test]
    public void Reader_GenerateDependencyDot_StillDrawsForeignKeys_AndSaysModulesMayBeMissing()
    {
        var result = Parse(Reader.GenerateDependencyDot(schema: null));

        Assert.That(result.GetProperty("edges").EnumerateArray()
                .Select(e => e.GetProperty("dependency_kind").GetString()),
            Has.Member("FOREIGN_KEY"));
        Assert.That(result.GetProperty("visibility").GetProperty("complete").GetBoolean(), Is.False);
    }

    [Test]
    public void Reader_CheckPermissions_NamesWhatIsMissing_WithTheGrant()
    {
        var report = Parse(Reader.CheckPermissions());

        var modules = Area(report, "module_definitions");
        Assert.That(modules.GetProperty("status").GetString(), Is.EqualTo("partial"));
        Assert.That(modules.GetProperty("grant").GetString(),
            Is.EqualTo($"GRANT VIEW DEFINITION TO [{TestDatabase.ReaderLogin}];"));

        Assert.That(Area(report, "tables_and_views").GetProperty("status").GetString(), Is.EqualTo("complete"));
        Assert.That(Area(report, "index_usage").GetProperty("status").GetString(), Is.EqualTo("partial"));
        // db_datareader carries SELECT on the database, which reaches sys.sql_expression_dependencies;
        // what is missing is the VIEW DEFINITION that decides which references it returns.
        Assert.That(Area(report, "sql_dependencies").GetProperty("status").GetString(), Is.EqualTo("partial"));

        Assert.That(report.GetProperty("grant_script").GetString(),
            Does.Contain($"USE [{TestDatabase.DatabaseName}];")
                .And.Contain($"GRANT VIEW DEFINITION TO [{TestDatabase.ReaderLogin}];")
                .And.Contain("USE master;"));
    }

    [Test]
    public void Reader_CheckPermissions_GrantsTheServerPermissionOfItsVersion()
    {
        var report = Parse(Reader.CheckPermissions());
        var major = report.GetProperty("identity").GetProperty("server_version").GetProperty("major").GetInt32();

        Assert.That(Area(report, "index_usage").GetProperty("grant").GetString(),
            Is.EqualTo($"USE master; GRANT {SchemaReader.ServerStatePermission(major)} TO [{TestDatabase.ReaderLogin}];"));
    }

    [Test]
    public void Reader_CheckPermissions_ListsSchemaLevelViewDefinition()
    {
        var schemas = Parse(Reader.CheckPermissions()).GetProperty("schema_view_definition");

        Assert.That(schemas.GetProperty("missing").EnumerateArray().Select(s => s.GetString()),
            Is.SupersetOf(new[] { "dbo", "financeiro" }));
    }

    [Test]
    public void Reader_CheckPermissions_IsReadOnly()
    {
        var writeAccess = Parse(Reader.CheckPermissions()).GetProperty("write_access");

        Assert.That(writeAccess.GetProperty("read_only").GetBoolean(), Is.True);
    }

    #endregion

    #region db_datareader with the recommended database permissions

    [Test]
    public void Viewer_Overview_SeesEveryProcedure()
    {
        var overview = Parse(Viewer.GetDatabaseOverview());

        Assert.That(overview.GetProperty("procedure_count").GetInt32(), Is.EqualTo(2));
        Assert.That(overview.GetProperty("visibility").GetProperty("complete").GetBoolean(), Is.True);
    }

    [Test]
    public void Viewer_ListProcedures_ReportsReferencesAndMetrics()
    {
        var row = Parse(Viewer.ListProcedures(schema: "financeiro"))[0];

        Assert.That(row.GetProperty("definition_visible").GetBoolean(), Is.True);
        Assert.That(row.GetProperty("line_count").GetInt32(), Is.GreaterThan(0));
        Assert.That(row.GetProperty("referenced_object_count").GetInt32(), Is.GreaterThan(0));
    }

    [Test]
    public void Viewer_GetTableUsage_FindsTheModulesThatUseTheTable()
    {
        var result = Parse(Viewer.GetTableUsage("Instrumento", schema: "dbo"));

        Assert.That(result.GetProperty("sql_module_usage").EnumerateArray()
                .Select(r => r.GetProperty("referencing_object").GetString()),
            Has.Member("vw_InstrumentosAtivos"));
        Assert.That(result.GetProperty("visibility").GetProperty("complete").GetBoolean(), Is.True);
    }

    [Test]
    public void Viewer_CheckPermissions_OnlyTheServerPermissionIsMissing()
    {
        var report = Parse(Viewer.CheckPermissions());

        Assert.That(Area(report, "module_definitions").GetProperty("status").GetString(), Is.EqualTo("complete"));
        Assert.That(Area(report, "sql_dependencies").GetProperty("status").GetString(), Is.EqualTo("complete"));
        Assert.That(Area(report, "index_usage").GetProperty("status").GetString(), Is.EqualTo("partial"));
        Assert.That(report.GetProperty("grant_script").GetString(), Does.StartWith("USE master;"));
        Assert.That(report.TryGetProperty("schema_view_definition", out _), Is.False);
    }

    #endregion

    #region Outside db_datareader, with a couple of object grants

    [Test]
    public void Narrow_ListProcedures_DoesNotFail_WhenDependenciesCannotBeRead()
    {
        var rows = Parse(Narrow.ListProcedures(schema: null)).EnumerateArray().ToList();

        var row = rows.Single();
        Assert.That(row.GetProperty("procedure_name").GetString(), Is.EqualTo("sp_ObterInstrumento"));
        Assert.That(row.GetProperty("definition_visible").GetBoolean(), Is.False);
        Assert.That(row.GetProperty("referenced_object_count").ValueKind, Is.EqualTo(JsonValueKind.Null));
    }

    [Test]
    public void Narrow_GetDependencyGraph_FailsSayingWhatIsMissing()
    {
        var ex = Assert.Throws<ToolInputException>(() => Narrow.GetDependencyGraph(schema: null));

        Assert.That(ex!.Message, Does.Contain("sys.sql_expression_dependencies"));
        Assert.That(ex.Hint, Does.Contain("check_permissions"));
    }

    [Test]
    public void Narrow_GetTableUsage_ModuleUsageIsUnknown_NotEmpty()
    {
        var result = Parse(Narrow.GetTableUsage("Instrumento", schema: "dbo"));

        Assert.That(result.GetProperty("sql_module_usage").ValueKind, Is.EqualTo(JsonValueKind.Null));
        Assert.That(result.GetProperty("visibility").GetProperty("notes")[0].GetString(),
            Does.Contain("sys.sql_expression_dependencies"));
    }

    [Test]
    public void Narrow_Overview_SaysTablesMayBeMissingToo()
    {
        var overview = Parse(Narrow.GetDatabaseOverview());

        Assert.That(overview.GetProperty("table_count").GetInt32(), Is.EqualTo(1));
        Assert.That(overview.GetProperty("visibility").GetProperty("notes").EnumerateArray()
                .Select(n => n.GetString()),
            Has.Some.Contains("tables and views it holds no permission on"));
    }

    [Test]
    public void Narrow_CheckPermissions_AsksForEveryDatabasePermission()
    {
        var report = Parse(Narrow.CheckPermissions());

        Assert.That(Area(report, "tables_and_views").GetProperty("status").GetString(), Is.EqualTo("partial"));
        Assert.That(Area(report, "sql_dependencies").GetProperty("status").GetString(), Is.EqualTo("unavailable"));
        Assert.That(Area(report, "sql_dependencies").GetProperty("grant").GetString(),
            Does.Contain($"GRANT SELECT ON sys.sql_expression_dependencies TO [{TestDatabase.NarrowLogin}];"));
        // The script joins the areas, and db_datareader already covers the dependency view.
        Assert.That(report.GetProperty("grant_script").GetString(),
            Does.Contain($"ALTER ROLE db_datareader ADD MEMBER [{TestDatabase.NarrowLogin}];")
                .And.Not.Contain("sys.sql_expression_dependencies"));
    }

    [Test]
    public void Narrow_CheckPermissions_ExecuteAloneDoesNotMakeItWritable()
    {
        var writeAccess = Parse(Narrow.CheckPermissions()).GetProperty("write_access");

        Assert.That(writeAccess.GetProperty("read_only").GetBoolean(), Is.True);
        Assert.That(writeAccess.GetProperty("schema_and_object_grants").EnumerateArray()
                .Select(g => g.GetProperty("permission").GetString()),
            Has.Member("EXECUTE"));
    }

    #endregion

    #region A "read-only" login with one object-level GRANT

    [Test]
    public void Inserter_CheckPermissions_IsNotReadOnly_BecauseOfAnObjectGrant()
    {
        var writeAccess = Parse(Inserter.CheckPermissions()).GetProperty("write_access");

        Assert.That(writeAccess.GetProperty("read_only").GetBoolean(), Is.False);
        Assert.That(writeAccess.GetProperty("roles").GetArrayLength(), Is.Zero,
            "No role gives it away; only the object-level GRANT does.");
        var insert = writeAccess.GetProperty("schema_and_object_grants").EnumerateArray()
            .Single(g => g.GetProperty("permission").GetString() == "INSERT");
        Assert.That(insert.GetProperty("scope").GetString(), Is.EqualTo("object"));
        Assert.That(insert.GetProperty("count").GetInt32(), Is.EqualTo(1));
    }

    #endregion
}
