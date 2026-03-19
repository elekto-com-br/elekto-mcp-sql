using System.Text.Json;
using Elekto.Mcp.Sql.Data;
using Elekto.Mcp.Sql.Tests.Infrastructure;

namespace Elekto.Mcp.Sql.Tests;

/// <summary>
/// Testes de integração do SchemaReader contra o LocalDB.
/// O banco ElektoMcpTest é criado uma vez por fixture e destruído ao final.
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
    public void CreateReader() => _reader = new SchemaReader(_db.ConnectionString);

    // helpers
    private static JsonElement[] ParseArray(string json) =>
        JsonSerializer.Deserialize<JsonElement[]>(json)!;

    private static JsonElement ParseObject(string json) =>
        JsonSerializer.Deserialize<JsonElement>(json);

    // =========================================================================
    // ListSchemas
    // =========================================================================

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

    // =========================================================================
    // ListTables
    // =========================================================================

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

    // =========================================================================
    // ListViews
    // =========================================================================

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

    // =========================================================================
    // ListProcedures
    // =========================================================================

    [Test]
    public void ListProcedures_ReturnsSp_ObterInstrumento()
    {
        var rows = ParseArray(_reader.ListProcedures(schema: null));
        var names = rows.Select(r => r.GetProperty("procedure_name").GetString()).ToList();

        Assert.That(names, Has.Member("sp_ObterInstrumento"));
    }

    // =========================================================================
    // ListFunctions
    // =========================================================================

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

    // =========================================================================
    // GetTableSchema
    // =========================================================================

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

    // =========================================================================
    // GetViewDefinition
    // =========================================================================

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

    // =========================================================================
    // GetProcedureDefinition
    // =========================================================================

    [Test]
    public void GetProcedureDefinition_ReturnsCreateText()
    {
        var rows = ParseArray(_reader.GetProcedureDefinition("sp_ObterInstrumento", schema: null));

        Assert.That(rows, Has.Length.EqualTo(1));
        Assert.That(rows[0].GetProperty("definition").GetString(),
            Does.Contain("sp_ObterInstrumento"));
    }

    // =========================================================================
    // GetFunctionDefinition
    // =========================================================================

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

    // =========================================================================
    // QueryTable
    // =========================================================================

    [Test]
    public void QueryTable_DefaultTop_Returns100OrLess()
    {
        var rows = ParseArray(_reader.QueryTable(
            "Instrumento", schema: null,
            columns: null, where: null, orderBy: null,
            top: 100, skip: 0, maxRows: 10_000));

        // Temos 5 linhas de seed, todas retornam
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
        // maxRows = 3, top = 100 => deve retornar no máximo 3
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
            "dbo.Instrumento", schema: null,  // ponto no nome = inválido
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
}
