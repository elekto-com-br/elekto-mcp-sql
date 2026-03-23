// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

using System.Text.Json;
using Elekto.Mcp.Sql.Data;
using Elekto.Mcp.Sql.Tests.Infrastructure;
using Microsoft.Data.SqlClient;

namespace Elekto.Mcp.Sql.Tests;

/// <summary>
/// Integration tests for SchemaReader against LocalDB.
/// The ElektoMcpTest database is created once per fixture and dropped on teardown.
/// </summary>
[TestFixture]
public class SchemaReaderTests
{
    private static TestDatabase _db = null!;
    private SchemaReader _reader = null!;

    [OneTimeSetUp]
    public static void CreateDatabase() => _db = new TestDatabase();

    [OneTimeTearDown]
    public static void DropDatabase() => _db.Dispose();

    [SetUp]
    public void CreateReader() => _reader = new SchemaReader(_db.ConnectionString, defaultTimeoutSeconds: 30);

    private static JsonElement[] ParseArray(string json) =>
        JsonSerializer.Deserialize<JsonElement[]>(json)!;

    private static JsonElement ParseObject(string json) =>
        JsonSerializer.Deserialize<JsonElement>(json);

    [Test]
    public void GetDatabaseOverview_ReturnsDatabaseName()
    {
        var result = ParseObject(_reader.GetDatabaseOverview());

        Assert.That(result.GetProperty("database_name").GetString(),
            Is.EqualTo(TestDatabase.DatabaseName));
    }

    [TestCase("table_count",     3)]
    [TestCase("view_count",      2)]
    [TestCase("procedure_count", 2)]
    [TestCase("function_count",  3)]
    [TestCase("schema_count",    2)]
    public void GetDatabaseOverview_ObjectCounts_MatchSeedSchema(string property, int expected)
    {
        var result = ParseObject(_reader.GetDatabaseOverview());

        Assert.That(result.GetProperty(property).GetInt32(), Is.EqualTo(expected));
    }

    [Test]
    public void GetDatabaseOverview_SizeMb_IsGreaterThanZero()
    {
        var result = ParseObject(_reader.GetDatabaseOverview());

        Assert.That(result.GetProperty("size_mb").GetDecimal(), Is.GreaterThan(0));
    }

    [TestCase("connected_user")]
    [TestCase("machine_name")]
    [TestCase("instance_name")]
    [TestCase("server_name")]
    public void GetDatabaseOverview_ServerMetadata_IsNotEmpty(string property)
    {
        var result = ParseObject(_reader.GetDatabaseOverview());

        Assert.That(result.GetProperty(property).GetString(), Is.Not.Null.And.Not.Empty);
    }

    [Test]
    public void ListSchemas_ReturnsUserSchemas()
    {
        var rows = ParseArray(_reader.ListSchemas());
        var names = rows.Select(r => r.GetProperty("schema_name").GetString()).ToList();

        Assert.That(names, Has.Member("dbo"));
        Assert.That(names, Has.Member("financeiro"));
    }

    [Test]
    public void ListSchemas_ExcludesSystemSchemas()
    {
        var rows = ParseArray(_reader.ListSchemas());
        var names = rows.Select(r => r.GetProperty("schema_name").GetString()).ToList();

        Assert.That(names, Has.No.Member("sys"));
        Assert.That(names, Has.No.Member("INFORMATION_SCHEMA"));
    }

    [Test]
    public void ListTables_NoFilter_ReturnsBothSchemas()
    {
        var rows = ParseArray(_reader.ListTables(schema: null));
        var names = rows.Select(r => r.GetProperty("table_name").GetString()).ToList();

        Assert.That(names, Has.Member("Instrumento"));
        Assert.That(names, Has.Member("Posicao"));
    }

    [Test]
    public void ListTables_FilterBySchema_ReturnsOnlyThatSchema()
    {
        var rows = ParseArray(_reader.ListTables(schema: "financeiro"));
        var schemas = rows.Select(r => r.GetProperty("schema_name").GetString()).Distinct().ToList();

        Assert.That(schemas, Is.EquivalentTo(new[] { "financeiro" }));
        Assert.That(rows.Select(r => r.GetProperty("table_name").GetString()),
            Has.Member("Posicao"));
    }

    [Test]
    public void ListTables_IncludesApproximateRowCount()
    {
        var rows = ParseArray(_reader.ListTables(schema: "dbo"));
        var instrumento = rows.First(r => r.GetProperty("table_name").GetString() == "Instrumento");

        Assert.That(instrumento.TryGetProperty("row_count_approx", out _), Is.True);
    }

    [Test]
    public void ListViews_NoFilter_ReturnsBothViews()
    {
        var rows = ParseArray(_reader.ListViews(schema: null));
        var names = rows.Select(r => r.GetProperty("view_name").GetString()).ToList();

        Assert.That(names, Has.Member("vw_InstrumentosAtivos"));
        Assert.That(names, Has.Member("vw_PosicaoDetalhada"));
    }

    [Test]
    public void ListViews_FilterBySchema_ReturnsOnlyThatSchema()
    {
        var rows = ParseArray(_reader.ListViews(schema: "dbo"));

        Assert.That(rows.Select(r => r.GetProperty("schema_name").GetString()),
            Has.All.EqualTo("dbo"));
    }

    [Test]
    public void ListProcedures_ReturnsSp_ObterInstrumento()
    {
        var rows = ParseArray(_reader.ListProcedures(schema: null));
        var names = rows.Select(r => r.GetProperty("procedure_name").GetString()).ToList();

        Assert.That(names, Has.Member("sp_ObterInstrumento"));
    }

    [Test]
    public void ListFunctions_ReturnsBothFunctions()
    {
        var rows = ParseArray(_reader.ListFunctions(schema: null));
        var names = rows.Select(r => r.GetProperty("function_name").GetString()).ToList();

        Assert.That(names, Has.Member("fn_DiasParaVencimento"));
        Assert.That(names, Has.Member("fn_InstrumentosVencendoEm"));
    }

    [Test]
    public void ListFunctions_ExposesCorrectTypes()
    {
        var rows = ParseArray(_reader.ListFunctions(schema: null));

        var scalar = rows.First(r => r.GetProperty("function_name").GetString() == "fn_DiasParaVencimento");
        var tvf    = rows.First(r => r.GetProperty("function_name").GetString() == "fn_InstrumentosVencendoEm");

        Assert.That(scalar.GetProperty("function_type").GetString(), Is.EqualTo("scalar"));
        Assert.That(tvf.GetProperty("function_type").GetString(),    Is.EqualTo("inline_table"));
    }

    #region GetTableSchema

    [Test]
    public void GetTableSchema_Instrumento_ReturnsAllColumns()
    {
        var result = ParseObject(_reader.GetTableSchema("Instrumento", schema: null));
        var columns = result.GetProperty("columns").EnumerateArray()
            .Select(c => c.GetProperty("column_name").GetString())
            .ToList();

        Assert.That(columns, Is.EquivalentTo(new[]
        {
            "InstrumentoId", "Codigo", "Descricao",
            "DataVencimento", "PrecoCusto", "Ativo"
        }));
    }

    [Test]
    public void GetTableSchema_Instrumento_IdentifiesIdentityColumn()
    {
        var result  = ParseObject(_reader.GetTableSchema("Instrumento", schema: null));
        var idCol   = result.GetProperty("columns").EnumerateArray()
            .First(c => c.GetProperty("column_name").GetString() == "InstrumentoId");

        Assert.That(idCol.GetProperty("is_identity").GetBoolean(), Is.True);
    }

    [Test]
    public void GetTableSchema_Instrumento_ReturnsPrimaryKey()
    {
        var result = ParseObject(_reader.GetTableSchema("Instrumento", schema: null));
        var pks    = result.GetProperty("primary_keys").EnumerateArray()
            .Select(r => r.GetProperty("column_name").GetString())
            .ToList();

        Assert.That(pks, Is.EqualTo(new[] { "InstrumentoId" }));
    }

    [Test]
    public void GetTableSchema_Posicao_ReturnsForeignKey()
    {
        var result = ParseObject(_reader.GetTableSchema("Posicao", schema: "financeiro"));
        var fks    = result.GetProperty("foreign_keys").EnumerateArray().ToList();

        Assert.That(fks, Has.Count.GreaterThanOrEqualTo(1));
        var fk = fks.First();
        Assert.That(fk.GetProperty("referenced_table").GetString(), Is.EqualTo("Instrumento"));
        Assert.That(fk.GetProperty("column_name").GetString(),      Is.EqualTo("InstrumentoId"));
    }

    [Test]
    public void GetTableSchema_Posicao_ReturnsIndex()
    {
        var result  = ParseObject(_reader.GetTableSchema("Posicao", schema: "financeiro"));
        var indexes = result.GetProperty("indexes").EnumerateArray()
            .Select(i => i.GetProperty("index_name").GetString())
            .ToList();

        Assert.That(indexes, Has.Member("IX_Posicao_DataRef"));
    }

    [Test]
    public void GetTableSchema_ColumnWithDescription_ReturnsExtendedProperty()
    {
        var result  = ParseObject(_reader.GetTableSchema("Instrumento", schema: null));
        var idCol   = result.GetProperty("columns").EnumerateArray()
            .First(c => c.GetProperty("column_name").GetString() == "InstrumentoId");

        Assert.That(idCol.GetProperty("description").GetString(),
            Is.EqualTo("Identificador único do instrumento"));
    }

    #endregion

    [Test]
    public void GetViewDefinition_ReturnsDefinitionText()
    {
        var result     = ParseObject(_reader.GetViewDefinition("vw_InstrumentosAtivos", schema: null));
        var definition = result.GetProperty("definition").EnumerateArray().First();

        Assert.That(definition.GetProperty("definition").GetString(),
            Does.Contain("vw_InstrumentosAtivos"));
    }

    [Test]
    public void GetViewDefinition_ReturnsColumns()
    {
        var result  = ParseObject(_reader.GetViewDefinition("vw_InstrumentosAtivos", schema: null));
        var columns = result.GetProperty("columns").EnumerateArray()
            .Select(c => c.GetProperty("column_name").GetString())
            .ToList();

        Assert.That(columns, Has.Member("Codigo"));
        Assert.That(columns, Has.Member("Descricao"));
    }

    [Test]
    public void GetProcedureDefinition_ReturnsCreateText()
    {
        var rows = ParseArray(_reader.GetProcedureDefinition("sp_ObterInstrumento", schema: null));

        Assert.That(rows, Has.Length.EqualTo(1));
        Assert.That(rows[0].GetProperty("definition").GetString(),
            Does.Contain("sp_ObterInstrumento"));
    }

    [Test]
    public void GetFunctionDefinition_Scalar_ReturnsDefinition()
    {
        var rows = ParseArray(_reader.GetFunctionDefinition("fn_DiasParaVencimento", schema: null));

        Assert.That(rows, Has.Length.EqualTo(1));
        Assert.That(rows[0].GetProperty("definition").GetString(),
            Does.Contain("fn_DiasParaVencimento"));
    }

    [Test]
    public void GetFunctionDefinition_InlineTVF_ReturnsDefinition()
    {
        var rows = ParseArray(_reader.GetFunctionDefinition("fn_InstrumentosVencendoEm", schema: null));

        Assert.That(rows, Has.Length.EqualTo(1));
        Assert.That(rows[0].GetProperty("definition").GetString(),
            Does.Contain("fn_InstrumentosVencendoEm"));
    }

    #region QueryTable

    [Test]
    public void QueryTable_DefaultTop_Returns100OrLess()
    {
        var rows = ParseArray(_reader.QueryTable(
            "Instrumento", schema: null,
            columns: null, where: null, orderBy: null,
            top: 100, skip: 0, maxRows: 10_000));

        // All 5 seeded rows are returned
        Assert.That(rows.Length, Is.EqualTo(5));
    }

    [Test]
    public void QueryTable_WithWhere_FiltersCorrectly()
    {
        var rows = ParseArray(_reader.QueryTable(
            "Instrumento", schema: null,
            columns: null, where: "Ativo = 1", orderBy: null,
            top: 100, skip: 0, maxRows: 10_000));

        Assert.That(rows.Length, Is.EqualTo(4));
    }

    [Test]
    public void QueryTable_WithColumns_ReturnsOnlySelectedColumns()
    {
        var rows = ParseArray(_reader.QueryTable(
            "Instrumento", schema: null,
            columns: "Codigo, Descricao", where: null, orderBy: null,
            top: 100, skip: 0, maxRows: 10_000));

        var first = rows[0];
        Assert.That(first.TryGetProperty("Codigo",    out _), Is.True);
        Assert.That(first.TryGetProperty("Descricao", out _), Is.True);
        Assert.That(first.TryGetProperty("Ativo",     out _), Is.False);
    }

    [Test]
    public void QueryTable_WithTopAndSkip_Paginates()
    {
        var page1 = ParseArray(_reader.QueryTable(
            "Instrumento", schema: null,
            columns: "InstrumentoId", where: null, orderBy: "InstrumentoId",
            top: 2, skip: 0, maxRows: 10_000));

        var page2 = ParseArray(_reader.QueryTable(
            "Instrumento", schema: null,
            columns: "InstrumentoId", where: null, orderBy: "InstrumentoId",
            top: 2, skip: 2, maxRows: 10_000));

        var ids1 = page1.Select(r => r.GetProperty("InstrumentoId").GetInt32()).ToList();
        var ids2 = page2.Select(r => r.GetProperty("InstrumentoId").GetInt32()).ToList();

        Assert.That(ids1, Has.Count.EqualTo(2));
        Assert.That(ids2, Has.Count.EqualTo(2));
        Assert.That(ids1.Intersect(ids2), Is.Empty);
    }

    [Test]
    public void QueryTable_TopExceedsMaxRows_IsClamped()
    {
        // maxRows = 3, top = 100 => at most 3 rows should be returned
        var rows = ParseArray(_reader.QueryTable(
            "Instrumento", schema: null,
            columns: null, where: null, orderBy: null,
            top: 100, skip: 0, maxRows: 3));

        Assert.That(rows.Length, Is.LessThanOrEqualTo(3));
    }

    [Test]
    public void QueryTable_InvalidTableIdentifier_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => _reader.QueryTable(
            "dbo.Instrumento", schema: null,  // dot in name = invalid
            columns: null, where: null, orderBy: null,
            top: 10, skip: 0, maxRows: 10_000));
    }

    [Test]
    public void QueryTable_SchemaFinanceiro_ReturnsRows()
    {
        var rows = ParseArray(_reader.QueryTable(
            "Posicao", schema: "financeiro",
            columns: null, where: null, orderBy: null,
            top: 100, skip: 0, maxRows: 10_000));

        Assert.That(rows.Length, Is.EqualTo(3));
    }

    [Test]
    public void QueryTable_GroupByAndAggregates_ReturnsAggregatedRows()
    {
        var rows = ParseArray(_reader.QueryTable(
            table: "Posicao",
            schema: "financeiro",
            columns: null,
            where: null,
            orderBy: "[InstrumentoId]",
            top: 100,
            skip: 0,
            maxRows: 10_000,
            groupBy: "InstrumentoId",
            aggregates: "SUM(Quantidade) AS QuantidadeTotal, MAX(ValorMercado) AS ValorMaximo",
            samplePercent: null));

        Assert.That(rows.Length, Is.EqualTo(3));
        Assert.That(rows[0].TryGetProperty("QuantidadeTotal", out _), Is.True);
    }

    [Test]
    public void QueryTable_WithSampling_ReturnsSubset()
    {
        var rows = ParseArray(_reader.QueryTable(
            table: "Instrumento",
            schema: "dbo",
            columns: "InstrumentoId",
            where: null,
            orderBy: null,
            top: 100,
            skip: 0,
            maxRows: 10_000,
            groupBy: null,
            aggregates: null,
            samplePercent: 40));

        Assert.That(rows.Length, Is.LessThanOrEqualTo(5));
    }

    [Test]
    public void QueryTable_InvalidAggregate_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => _reader.QueryTable(
            table: "Instrumento",
            schema: "dbo",
            columns: null,
            where: null,
            orderBy: null,
            top: 10,
            skip: 0,
            maxRows: 10_000,
            groupBy: "Codigo",
            aggregates: "MEDIAN(PrecoCusto)",
            samplePercent: null));
    }

    [Test]
    public void QueryTable_SqlError_IsWrappedWithObjectName()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => _reader.QueryTable(
            table: "Instrumento",
            schema: "dbo",
            columns: "NaoExiste",
            where: null,
            orderBy: null,
            top: 10,
            skip: 0,
            maxRows: 10_000,
            groupBy: null,
            aggregates: null,
            samplePercent: null));

        Assert.That(ex!.Message, Does.Contain("[dbo].[Instrumento]"));
    }

    #endregion

    [Test]
    public void GetDependencyGraph_ReturnsForeignKeyAndSqlDependencies()
    {
        var rows = ParseArray(_reader.GetDependencyGraph("financeiro"));

        Assert.That(rows.Any(r =>
            r.GetProperty("dependency_kind").GetString() == "FOREIGN_KEY" &&
            r.GetProperty("from_object").GetString() == "Posicao" &&
            r.GetProperty("to_object").GetString() == "Instrumento"), Is.True);

        Assert.That(rows.Any(r =>
            r.GetProperty("dependency_kind").GetString() == "SQL_EXPRESSION" &&
            r.GetProperty("from_object").GetString() == "sp_ResumoPosicao"), Is.True);
    }

    [Test]
    public void GetTableUsage_ReturnsModuleAndForeignKeyReferences()
    {
        var usage = ParseObject(_reader.GetTableUsage("Instrumento", "dbo"));

        var fkUsage = usage.GetProperty("foreign_key_usage").EnumerateArray().ToArray();
        var moduleUsage = usage.GetProperty("sql_module_usage").EnumerateArray().ToArray();

        Assert.That(fkUsage.Any(r => r.GetProperty("referencing_object").GetString() == "Posicao"), Is.True);
        Assert.That(moduleUsage.Any(r => r.GetProperty("referencing_object").GetString() == "vw_PosicaoDetalhada"), Is.True);
    }

    [Test]
    public void GetDataProfile_ReturnsNullRatioDistinctAndTopValues()
    {
        var profile = ParseObject(_reader.GetDataProfile("Instrumento", "dbo", "Codigo,DataVencimento", 3));
        var columns = profile.GetProperty("columns").EnumerateArray().ToArray();

        var codigo = columns.First(c => c.GetProperty("column_name").GetString() == "Codigo");
        var vencimento = columns.First(c => c.GetProperty("column_name").GetString() == "DataVencimento");

        Assert.Multiple(() =>
        {
            Assert.That(codigo.GetProperty("distinct_count").GetInt64(), Is.EqualTo(5));
            Assert.That(codigo.GetProperty("top_values").GetArrayLength(), Is.EqualTo(3));
            Assert.That(vencimento.GetProperty("null_count").GetInt64(), Is.EqualTo(1));
            Assert.That(vencimento.GetProperty("null_ratio").GetDecimal(), Is.GreaterThan(0m));
        });
    }

    [Test]
    public void GetIndexHealth_ReturnsDuplicateIndexCandidate()
    {
        var health = ParseObject(_reader.GetIndexHealth("financeiro"));
        var duplicates = health.GetProperty("duplicate_indexes").EnumerateArray().ToArray();

        Assert.That(duplicates.Any(d =>
            d.GetProperty("table_name").GetString() == "Posicao" &&
            d.GetProperty("index_a").GetString() == "IX_Posicao_Instrumento"), Is.True);
    }

    [Test]
    public void GenerateDependencyDot_ReturnsDotAndNodeKinds()
    {
        var graph = ParseObject(_reader.GenerateDependencyDot("financeiro"));
        var dot = graph.GetProperty("dot").GetString();
        var nodes = graph.GetProperty("nodes").EnumerateArray().ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(graph.GetProperty("format").GetString(), Is.EqualTo("dot"));
            Assert.That(dot, Does.Contain("digraph dependencies"));
            Assert.That(dot, Does.Contain("dependency_kind"));
            Assert.That(nodes.Any(n =>
                n.GetProperty("object_name").GetString() == "Posicao" &&
                n.GetProperty("node_kind").GetString() == "TABLE"), Is.True);
            Assert.That(nodes.Any(n =>
                n.GetProperty("object_name").GetString() == "vw_PosicaoDetalhada" &&
                n.GetProperty("node_kind").GetString() == "VIEW"), Is.True);
            Assert.That(nodes.Any(n =>
                n.GetProperty("object_name").GetString() == "sp_ResumoPosicao" &&
                n.GetProperty("node_kind").GetString() == "PROCEDURE"), Is.True);
            Assert.That(nodes.Any(n =>
                n.GetProperty("object_name").GetString() == "fn_PosicoesAcimaDe" &&
                n.GetProperty("node_kind").GetString() == "FUNCTION"), Is.True);
        });
    }

    [Test]
    public void CompareSchemas_ReturnsDifferences()
    {
        var tempDatabase = $"ElektoMcpTest_Compare_{Guid.NewGuid():N}";
        try
        {
            CreateCompareDatabase(tempDatabase);
            var targetConn = $"Server={TestDatabase.InstanceName};Database={tempDatabase};Integrated Security=SSPI;TrustServerCertificate=True";
            var targetReader = new SchemaReader(targetConn);

            var result = ParseObject(SchemaReader.CompareSchemas(_reader, targetReader, "financeiro", "financeiro"));
            var missing = result.GetProperty("missing_tables_in_target").EnumerateArray().Select(x => x.GetString()).ToList();
            var diffs = result.GetProperty("table_column_differences").EnumerateArray().ToArray();

            Assert.That(missing, Has.Member("financeiro.MovimentoPosicao"));
            Assert.That(diffs.Any(d => d.GetProperty("table_name").GetString() == "financeiro.Posicao"), Is.True);
        }
        finally
        {
            DropDatabaseIfExists(tempDatabase);
        }
    }

    private static void CreateCompareDatabase(string databaseName)
    {
        using var master = new SqlConnection($"Server={TestDatabase.InstanceName};Database=master;Integrated Security=SSPI;TrustServerCertificate=True");
        master.Open();
        Execute(master, $"CREATE DATABASE [{databaseName}];");

        using var db = new SqlConnection($"Server={TestDatabase.InstanceName};Database={databaseName};Integrated Security=SSPI;TrustServerCertificate=True");
        db.Open();

        Execute(db, "CREATE SCHEMA financeiro;");
        Execute(db, """
            CREATE TABLE financeiro.Posicao (
                PosicaoId     INT NOT NULL IDENTITY(1,1),
                InstrumentoId INT NOT NULL,
                Quantidade    DECIMAL(18,4) NOT NULL,
                DataRef       DATE NOT NULL,
                CONSTRAINT PK_Posicao PRIMARY KEY (PosicaoId)
            );
            """);
    }

    private static void DropDatabaseIfExists(string databaseName)
    {
        using var conn = new SqlConnection($"Server={TestDatabase.InstanceName};Database=master;Integrated Security=SSPI;TrustServerCertificate=True");
        conn.Open();

        Execute(conn, $"""
            IF EXISTS (SELECT 1 FROM sys.databases WHERE name = '{databaseName}')
            BEGIN
                ALTER DATABASE [{databaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{databaseName}];
            END
            """);
    }

    private static void Execute(SqlConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }
}
