// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

using Microsoft.Data.SqlClient;

namespace Elekto.Mcp.Sql.Tests.Infrastructure;

/// <summary>
/// Creates and destroys the ElektoMcpTest database in LocalDB.
/// Used as OneTimeSetUp / OneTimeTearDown through the TestDatabase fixture.
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
        // Recreates from scratch to ensure a clean state
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

        Execute(conn, "CREATE SCHEMA financeiro;");

        Execute(conn, """
            CREATE TABLE dbo.Instrumento (
                InstrumentoId   INT           NOT NULL IDENTITY(1,1),
                Codigo          VARCHAR(20)   NOT NULL,
                Descricao       NVARCHAR(200) NOT NULL,
                DataVencimento  DATE          NULL,
                PrecoCusto      DECIMAL(18,6) NOT NULL DEFAULT 0,
                Ativo           BIT           NOT NULL DEFAULT 1,
                CONSTRAINT PK_Instrumento PRIMARY KEY (InstrumentoId),
                CONSTRAINT UQ_Instrumento_Codigo UNIQUE (Codigo),
                CONSTRAINT CK_Instrumento_PrecoCusto CHECK (PrecoCusto >= 0)
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

        Execute(conn, """
            CREATE TABLE financeiro.Posicao (
                PosicaoId      INT            NOT NULL IDENTITY(1,1),
                InstrumentoId  INT            NOT NULL,
                Quantidade     DECIMAL(18,6)  NOT NULL,
                PrecoUnitario  DECIMAL(18,6)  NOT NULL DEFAULT 0,
                ValorMercado   AS (Quantidade * PrecoUnitario),
                DataRef        DATE           NOT NULL,
                CONSTRAINT PK_Posicao PRIMARY KEY (PosicaoId),
                CONSTRAINT CK_Posicao_Quantidade CHECK (Quantidade >= 0),
                CONSTRAINT FK_Posicao_Instrumento
                    FOREIGN KEY (InstrumentoId)
                    REFERENCES dbo.Instrumento (InstrumentoId)
                    ON DELETE NO ACTION ON UPDATE NO ACTION
            );
            """);

        Execute(conn, """
            CREATE TABLE financeiro.MovimentoPosicao (
                MovimentoId   INT           NOT NULL IDENTITY(1,1),
                PosicaoId     INT           NOT NULL,
                Tipo          CHAR(1)       NOT NULL,
                Quantidade    DECIMAL(18,6) NOT NULL,
                DataEvento    DATETIME2     NOT NULL,
                CONSTRAINT PK_MovimentoPosicao PRIMARY KEY (MovimentoId),
                CONSTRAINT CK_MovimentoPosicao_Tipo CHECK (Tipo IN ('C','V')),
                CONSTRAINT FK_MovimentoPosicao_Posicao
                    FOREIGN KEY (PosicaoId)
                    REFERENCES financeiro.Posicao (PosicaoId)
                    ON DELETE CASCADE
            );
            """);

        Execute(conn, "CREATE INDEX IX_Posicao_DataRef ON financeiro.Posicao (DataRef DESC);");
        Execute(conn, "CREATE INDEX IX_Posicao_Instrumento ON financeiro.Posicao (InstrumentoId);");
        Execute(conn, "CREATE INDEX IX_Posicao_Instrumento_Duplicado ON financeiro.Posicao (InstrumentoId);");

        Execute(conn, """
            CREATE VIEW dbo.vw_InstrumentosAtivos AS
            SELECT InstrumentoId, Codigo, Descricao, DataVencimento
            FROM   dbo.Instrumento
            WHERE  Ativo = 1;
            """);

        Execute(conn, """
            CREATE VIEW financeiro.vw_PosicaoDetalhada AS
            SELECT p.PosicaoId,
                   p.DataRef,
                   i.Codigo,
                   i.Descricao,
                   p.Quantidade,
                   p.PrecoUnitario,
                   p.ValorMercado
            FROM   financeiro.Posicao p
            JOIN   dbo.Instrumento    i ON p.InstrumentoId = i.InstrumentoId;
            """);

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

        Execute(conn, """
            CREATE PROCEDURE financeiro.sp_ResumoPosicao
                @DataRef DATE
            AS
            BEGIN
                SET NOCOUNT ON;
                SELECT i.Codigo,
                       SUM(p.Quantidade) AS QuantidadeTotal,
                       SUM(p.ValorMercado) AS ValorTotal
                FROM financeiro.Posicao p
                INNER JOIN dbo.Instrumento i ON i.InstrumentoId = p.InstrumentoId
                WHERE p.DataRef = @DataRef
                GROUP BY i.Codigo;
            END;
            """);

        Execute(conn, """
            CREATE FUNCTION dbo.fn_DiasParaVencimento (@DataVencimento DATE)
            RETURNS INT
            AS
            BEGIN
                RETURN DATEDIFF(DAY, CAST(GETDATE() AS DATE), @DataVencimento);
            END;
            """);

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

        Execute(conn, """
            CREATE FUNCTION financeiro.fn_PosicoesAcimaDe (@ValorMinimo DECIMAL(18,6))
            RETURNS @Resultado TABLE
            (
                PosicaoId INT NOT NULL,
                ValorMercado DECIMAL(18,6) NOT NULL
            )
            AS
            BEGIN
                INSERT INTO @Resultado (PosicaoId, ValorMercado)
                SELECT p.PosicaoId, p.ValorMercado
                FROM financeiro.Posicao p
                WHERE p.ValorMercado >= @ValorMinimo;
                RETURN;
            END;
            """);

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
            INSERT INTO financeiro.Posicao (InstrumentoId, Quantidade, PrecoUnitario, DataRef)
            VALUES
                (1, 1000.000000, 37.10, '2025-01-02'),
                (2,  500.000000, 69.90, '2025-01-02'),
                (4,  250.000000, 34.00, '2025-01-02');
            """);

        Execute(conn, """
            INSERT INTO financeiro.MovimentoPosicao (PosicaoId, Tipo, Quantidade, DataEvento)
            VALUES
                (1, 'C', 1000.000000, '2025-01-02T10:00:00'),
                (2, 'C', 500.000000, '2025-01-02T10:00:00'),
                (3, 'C', 250.000000, '2025-01-02T10:00:00');
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
