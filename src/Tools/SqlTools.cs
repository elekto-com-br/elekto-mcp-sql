using System.ComponentModel;
using Elekto.Mcp.Sql.Configuration;
using Elekto.Mcp.Sql.Data;
using ModelContextProtocol.Server;

namespace Elekto.Mcp.Sql.Tools;

/// <summary>
/// Tools MCP para introspecção e consulta de bancos SQL Server.
/// Todas as operações são somente leitura.
/// </summary>
[McpServerToolType]
public sealed class SqlTools
{
    private readonly ConnectionConfig _config;

    public SqlTools(ConnectionConfig config)
    {
        _config = config;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private SchemaReader GetReader(string database)
    {
        if (!_config.Databases.TryGetValue(database, out var entry))
        {
            var available = string.Join(", ", _config.Databases.Keys);
            throw new ArgumentException(
                $"Banco '{database}' não encontrado. Disponíveis: {available}");
        }
        return new SchemaReader(entry.ConnectionString);
    }

    private int GetMaxRows(string database) =>
        _config.Databases.TryGetValue(database, out var e) ? e.MaxQueryRows : 10_000;

    // -------------------------------------------------------------------------
    // Metadados gerais
    // -------------------------------------------------------------------------

    [McpServerTool, Description(
        "Lista os bancos de dados registrados na configuração do servidor MCP. " +
        "Use esta ferramenta primeiro para descobrir quais bancos estão disponíveis.")]
    public string list_databases()
    {
        var entries = _config.Databases.Select(kv => new
        {
            name         = kv.Key,
            max_query_rows = kv.Value.MaxQueryRows
        });
        return System.Text.Json.JsonSerializer.Serialize(entries);
    }

    [McpServerTool, Description(
        "Lista os schemas disponíveis em um banco de dados SQL Server, " +
        "excluindo schemas de sistema. Retorna nome do schema e proprietário.")]
    public string list_schemas(
        [Description("Nome do banco conforme registrado na configuração.")] string database)
        => GetReader(database).ListSchemas();

    // -------------------------------------------------------------------------
    // Tabelas
    // -------------------------------------------------------------------------

    [McpServerTool, Description(
        "Lista todas as tabelas de usuário de um banco de dados, " +
        "com schema e contagem aproximada de linhas.")]
    public string list_tables(
        [Description("Nome do banco conforme registrado na configuração.")]
        string database,
        [Description("Filtra pelo nome do schema (opcional).")]
        string? schema = null)
        => GetReader(database).ListTables(schema);

    [McpServerTool, Description(
        "Retorna o schema completo de uma tabela: colunas (tipo, nullable, identidade, " +
        "valor default, descrição), chaves primárias, chaves estrangeiras e índices.")]
    public string get_table_schema(
        [Description("Nome do banco conforme registrado na configuração.")]
        string database,
        [Description("Nome da tabela.")]
        string table,
        [Description("Schema da tabela (opcional, default dbo).")]
        string? schema = null)
        => GetReader(database).GetTableSchema(table, schema);

    // -------------------------------------------------------------------------
    // Views
    // -------------------------------------------------------------------------

    [McpServerTool, Description(
        "Lista todas as views de usuário de um banco de dados.")]
    public string list_views(
        [Description("Nome do banco conforme registrado na configuração.")]
        string database,
        [Description("Filtra pelo nome do schema (opcional).")]
        string? schema = null)
        => GetReader(database).ListViews(schema);

    [McpServerTool, Description(
        "Retorna a definição DDL (CREATE VIEW) e as colunas de uma view.")]
    public string get_view_definition(
        [Description("Nome do banco conforme registrado na configuração.")]
        string database,
        [Description("Nome da view.")]
        string view,
        [Description("Schema da view (opcional, default dbo).")]
        string? schema = null)
        => GetReader(database).GetViewDefinition(view, schema);

    // -------------------------------------------------------------------------
    // Stored procedures
    // -------------------------------------------------------------------------

    [McpServerTool, Description(
        "Lista todas as stored procedures de usuário de um banco de dados.")]
    public string list_procedures(
        [Description("Nome do banco conforme registrado na configuração.")]
        string database,
        [Description("Filtra pelo nome do schema (opcional).")]
        string? schema = null)
        => GetReader(database).ListProcedures(schema);

    [McpServerTool, Description(
        "Retorna o texto de definição (CREATE PROCEDURE) de uma stored procedure.")]
    public string get_procedure_definition(
        [Description("Nome do banco conforme registrado na configuração.")]
        string database,
        [Description("Nome da stored procedure.")]
        string procedure,
        [Description("Schema da procedure (opcional, default dbo).")]
        string? schema = null)
        => GetReader(database).GetProcedureDefinition(procedure, schema);

    // -------------------------------------------------------------------------
    // Funções
    // -------------------------------------------------------------------------

    [McpServerTool, Description(
        "Lista todas as funções de usuário (scalar, inline table-valued, " +
        "multi-statement table-valued) de um banco de dados.")]
    public string list_functions(
        [Description("Nome do banco conforme registrado na configuração.")]
        string database,
        [Description("Filtra pelo nome do schema (opcional).")]
        string? schema = null)
        => GetReader(database).ListFunctions(schema);

    [McpServerTool, Description(
        "Retorna o texto de definição (CREATE FUNCTION) de uma função de usuário.")]
    public string get_function_definition(
        [Description("Nome do banco conforme registrado na configuração.")]
        string database,
        [Description("Nome da função.")]
        string function,
        [Description("Schema da função (opcional, default dbo).")]
        string? schema = null)
        => GetReader(database).GetFunctionDefinition(function, schema);

    // -------------------------------------------------------------------------
    // Consulta de dados (somente SELECT em tabelas/views)
    // -------------------------------------------------------------------------

    [McpServerTool, Description(
        "Executa um SELECT em uma tabela ou view. Suporta filtragem, ordenação e paginação. " +
        "Máximo de linhas configurável por banco (padrão 10.000). " +
        "Use 'top' e 'skip' para amostrar tabelas grandes sem sobrecarregar o servidor. " +
        "Não executa DML (INSERT/UPDATE/DELETE) nem stored procedures.")]
    public string query_table(
        [Description("Nome do banco conforme registrado na configuração.")]
        string database,
        [Description("Nome da tabela ou view.")]
        string table,
        [Description("Schema da tabela/view (opcional, default dbo).")]
        string? schema = null,
        [Description("Lista de colunas separadas por vírgula. Use * para todas (padrão).")]
        string? columns = null,
        [Description("Cláusula WHERE sem a palavra-chave WHERE. Ex: 'Status = 1 AND DataRef >= ''2024-01-01'''")]
        string? where = null,
        [Description("Cláusula ORDER BY sem a palavra-chave ORDER BY. Ex: 'DataRef DESC'")]
        string? order_by = null,
        [Description("Número máximo de linhas a retornar (padrão 100, máximo configurável por banco).")]
        int top = 100,
        [Description("Número de linhas a pular antes de começar a retornar (para paginação, padrão 0).")]
        int skip = 0)
        => GetReader(database).QueryTable(table, schema, columns, where, order_by, top, skip, GetMaxRows(database));
}
