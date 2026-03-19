using Microsoft.Data.SqlClient;

namespace Elekto.Mcp.Sql.Tests.Infrastructure;

/// <summary>
/// Cria e destrói o banco de testes ElektoMcpTest no LocalDB.
/// Usado como OneTimeSetUp / OneTimeTearDown via TestDatabase.
/// </summary>
public sealed class TestDatabase : IDisposable
{
    public const string InstanceName  = @"(localdb)\MSSQLLocalDB";
    public const string DatabaseName  = "ElektoMcpTest";

    public string ConnectionString { get; } =
        $"Server={InstanceName};Database={DatabaseName};Integrated Security=SSPI;TrustServerCertificate=True";

    private readonly string _masterConn =
        $"Server={InstanceName};Database=master;Integrated Security=SSPI;TrustServerCertificate=True";

    public TestDatabase()
    {
        CreateDatabase();
        CreateSchema();
    }

    private void CreateDatabase()
    {
        using var conn = new SqlConnection(_masterConn);
        conn.Open();
        // Recria do zero para garantir estado limpo
        Execute(conn, $"""
            IF EXISTS (SELECT 1 FROM sys.databases WHERE name = '{DatabaseName}')
            BEGIN
                ALTER DATABASE [{DatabaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{DatabaseName}];
            END
            CREATE DATABASE [{DatabaseName}];
            """);
    }

    private void CreateSchema()
    {
        using var conn = new SqlConnection(ConnectionString);
        conn.Open();

        // Schema extra para testar filtro por schema
        Execute(conn, "CREATE SCHEMA financeiro;");

        // Tabela principal com todos os tipos de metadados relevantes
        Execute(conn, """
            CREATE TABLE dbo.Instrumento (
                InstrumentoId   INT           NOT NULL IDENTITY(1,1),
                Codigo          VARCHAR(20)   NOT NULL,
                Descricao       NVARCHAR(200) NOT NULL,
                DataVencimento  DATE          NULL,
                PrecoCusto      DECIMAL(18,6) NOT NULL DEFAULT 0,
                Ativo           BIT           NOT NULL DEFAULT 1,
                CONSTRAINT PK_Instrumento PRIMARY KEY (InstrumentoId),
                CONSTRAINT UQ_Instrumento_Codigo UNIQUE (Codigo)
            );
            """);

        Execute(conn, """
            EXEC sp_addextendedproperty
                'MS_Description', N'Cadastro de instrumentos financeiros',
                'SCHEMA', 'dbo', 'TABLE', 'Instrumento';
            """);

        Execute(conn, """
            EXEC sp_addextendedproperty
                'MS_Description', N'Identificador único do instrumento',
                'SCHEMA', 'dbo', 'TABLE', 'Instrumento', 'COLUMN', 'InstrumentoId';
            """);

        // Tabela no schema financeiro para testar filtro
        Execute(conn, """
            CREATE TABLE financeiro.Posicao (
                PosicaoId     INT          NOT NULL IDENTITY(1,1),
                InstrumentoId INT          NOT NULL,
                Quantidade    DECIMAL(18,6) NOT NULL,
                DataRef       DATE         NOT NULL,
                CONSTRAINT PK_Posicao PRIMARY KEY (PosicaoId),
                CONSTRAINT FK_Posicao_Instrumento
                    FOREIGN KEY (InstrumentoId)
                    REFERENCES dbo.Instrumento (InstrumentoId)
                    ON DELETE NO ACTION ON UPDATE NO ACTION
            );
            """);

        Execute(conn, "CREATE INDEX IX_Posicao_DataRef ON financeiro.Posicao (DataRef DESC);");

        // View simples
        Execute(conn, """
            CREATE VIEW dbo.vw_InstrumentosAtivos AS
            SELECT InstrumentoId, Codigo, Descricao, DataVencimento
            FROM   dbo.Instrumento
            WHERE  Ativo = 1;
            """);

        // View no schema financeiro
        Execute(conn, """
            CREATE VIEW financeiro.vw_PosicaoDetalhada AS
            SELECT p.PosicaoId,
                   p.DataRef,
                   i.Codigo,
                   i.Descricao,
                   p.Quantidade
            FROM   financeiro.Posicao p
            JOIN   dbo.Instrumento    i ON p.InstrumentoId = i.InstrumentoId;
            """);

        // Stored procedure
        Execute(conn, """
            CREATE PROCEDURE dbo.sp_ObterInstrumento
                @InstrumentoId INT
            AS
            BEGIN
                SET NOCOUNT ON;
                SELECT InstrumentoId, Codigo, Descricao, DataVencimento, PrecoCusto, Ativo
                FROM   dbo.Instrumento
                WHERE  InstrumentoId = @InstrumentoId;
            END;
            """);

        // Função scalar
        Execute(conn, """
            CREATE FUNCTION dbo.fn_DiasParaVencimento (@DataVencimento DATE)
            RETURNS INT
            AS
            BEGIN
                RETURN DATEDIFF(DAY, CAST(GETDATE() AS DATE), @DataVencimento);
            END;
            """);

        // Função table-valued inline
        Execute(conn, """
            CREATE FUNCTION dbo.fn_InstrumentosVencendoEm (@Dias INT)
            RETURNS TABLE
            AS
            RETURN (
                SELECT InstrumentoId, Codigo, Descricao, DataVencimento
                FROM   dbo.Instrumento
                WHERE  DataVencimento <= DATEADD(DAY, @Dias, CAST(GETDATE() AS DATE))
                  AND  Ativo = 1
            );
            """);

        // Dados de amostra para testar query_table
        Execute(conn, """
            INSERT INTO dbo.Instrumento (Codigo, Descricao, DataVencimento, PrecoCusto, Ativo)
            VALUES
                ('PETR4',  N'Petrobras PN',          '2026-01-15', 36.52, 1),
                ('VALE3',  N'Vale ON',                '2026-03-20', 68.10, 1),
                ('BBAS3',  N'Banco do Brasil ON',     '2025-12-31', 25.80, 0),
                ('ITUB4',  N'Itaú Unibanco PN',       NULL,         33.45, 1),
                ('WEGE3',  N'Weg ON',                 '2027-06-30', 42.00, 1);
            """);

        Execute(conn, """
            INSERT INTO financeiro.Posicao (InstrumentoId, Quantidade, DataRef)
            VALUES
                (1, 1000.000000, '2025-01-02'),
                (2,  500.000000, '2025-01-02'),
                (4,  250.000000, '2025-01-02');
            """);
    }

    private static void Execute(SqlConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    public void Dispose()
    {
        using var conn = new SqlConnection(_masterConn);
        conn.Open();
        Execute(conn, $"""
            IF EXISTS (SELECT 1 FROM sys.databases WHERE name = '{DatabaseName}')
            BEGIN
                ALTER DATABASE [{DatabaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{DatabaseName}];
            END
            """);
    }
}
