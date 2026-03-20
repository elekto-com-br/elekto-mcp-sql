// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

using System.Data;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;

namespace Elekto.Mcp.Sql.Data;

/// <summary>
/// Encapsulates all SQL Server 2022+ queries.
/// Uses sys.* views instead of INFORMATION_SCHEMA for richer and more precise metadata.
/// </summary>
public sealed class SchemaReader
{
    private readonly string _connectionString;

    public SchemaReader(string connectionString)
    {
        _connectionString = connectionString;
    }

    private SqlConnection OpenConnection()
    {
        var conn = new SqlConnection(_connectionString);
        conn.Open();
        return conn;
    }

    /// <summary>Executes a query and serializes the result as a JSON array of objects.</summary>
    private static string QueryToJson(SqlCommand cmd)
    {
        using var reader = cmd.ExecuteReader();
        var rows = new List<Dictionary<string, object?>>();
        while (reader.Read())
        {
            var row = new Dictionary<string, object?>();
            for (int i = 0; i < reader.FieldCount; i++)
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            rows.Add(row);
        }
        return JsonSerializer.Serialize(rows, new JsonSerializerOptions { WriteIndented = false });
    }

    #region Listings

    /// <summary>Returns a single-object overview of the current database.</summary>
    public string GetDatabaseOverview()
    {
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT
                DB_NAME()                                                                AS database_name,
                SUSER_SNAME()                                                            AS connected_user,
                CAST(SERVERPROPERTY('MachineName')   AS NVARCHAR(128))                   AS machine_name,
                ISNULL(CAST(SERVERPROPERTY('InstanceName') AS NVARCHAR(128)), 'DEFAULT') AS instance_name,
                @@SERVERNAME                                                             AS server_name,
                (SELECT COUNT(*) FROM sys.tables)                                        AS table_count,
                (SELECT COUNT(*) FROM sys.views)                                         AS view_count,
                (SELECT COUNT(*) FROM sys.procedures)                                    AS procedure_count,
                (SELECT COUNT(*) FROM sys.objects WHERE type IN ('FN','IF','TF'))        AS function_count,
                (SELECT COUNT(*) FROM sys.schemas
                 WHERE name NOT IN ('sys','guest','INFORMATION_SCHEMA',
                                    'db_owner','db_accessadmin','db_securityadmin',
                                    'db_ddladmin','db_backupoperator','db_datareader',
                                    'db_datawriter','db_denydatareader','db_denydatawriter')) AS schema_count,
                (SELECT CAST(SUM(size) * 8.0 / 1024 AS DECIMAL(10,2))
                 FROM sys.database_files)                                                AS size_mb;
            """;

        using var reader = cmd.ExecuteReader();
        if (!reader.Read()) return "{}";

        var row = new Dictionary<string, object?>();
        for (int i = 0; i < reader.FieldCount; i++)
            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);

        return JsonSerializer.Serialize(row);
    }

    public string ListSchemas()
    {
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT s.name AS schema_name,
                   p.name AS owner
            FROM sys.schemas s
            JOIN sys.database_principals p ON s.principal_id = p.principal_id
            WHERE s.name NOT IN ('sys','guest','INFORMATION_SCHEMA',
                                 'db_owner','db_accessadmin','db_securityadmin',
                                 'db_ddladmin','db_backupoperator','db_datareader',
                                 'db_datawriter','db_denydatareader','db_denydatawriter')
            ORDER BY s.name;
            """;
        return QueryToJson(cmd);
    }

    public string ListTables(string? schema)
    {
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT s.name AS schema_name,
                   t.name AS table_name,
                   p.rows  AS row_count_approx
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0,1)
            WHERE (@schema IS NULL OR s.name = @schema)
            ORDER BY s.name, t.name;
            """;
        cmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        return QueryToJson(cmd);
    }

    public string ListViews(string? schema)
    {
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT s.name AS schema_name,
                   v.name AS view_name,
                   v.is_replicated,
                   v.with_check_option
            FROM sys.views v
            JOIN sys.schemas s ON v.schema_id = s.schema_id
            WHERE (@schema IS NULL OR s.name = @schema)
            ORDER BY s.name, v.name;
            """;
        cmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        return QueryToJson(cmd);
    }

    public string ListProcedures(string? schema)
    {
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT s.name      AS schema_name,
                   p.name      AS procedure_name,
                   p.create_date,
                   p.modify_date
            FROM sys.procedures p
            JOIN sys.schemas s ON p.schema_id = s.schema_id
            WHERE (@schema IS NULL OR s.name = @schema)
            ORDER BY s.name, p.name;
            """;
        cmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        return QueryToJson(cmd);
    }

    public string ListFunctions(string? schema)
    {
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT s.name AS schema_name,
                   o.name AS function_name,
                   CASE o.type
                       WHEN 'FN'  THEN 'scalar'
                       WHEN 'IF'  THEN 'inline_table'
                       WHEN 'TF'  THEN 'multi_statement_table'
                   END AS function_type,
                   o.create_date,
                   o.modify_date
            FROM sys.objects o
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE o.type IN ('FN','IF','TF')
              AND (@schema IS NULL OR s.name = @schema)
            ORDER BY s.name, o.name;
            """;
        cmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        return QueryToJson(cmd);
    }

    #endregion

    #region Table Schema

    public string GetTableSchema(string table, string? schema)
    {
        using var conn = OpenConnection();

        // Colunas
        using var cmdCols = conn.CreateCommand();
        cmdCols.CommandText = """
            SELECT c.column_id,
                   c.name                                          AS column_name,
                   tp.name                                         AS data_type,
                   c.max_length,
                   c.precision,
                   c.scale,
                   c.is_nullable,
                   c.is_identity,
                   c.is_computed,
                   dc.definition                                   AS default_value,
                   ep.value                                        AS description
            FROM sys.columns c
            JOIN sys.objects  o  ON c.object_id  = o.object_id
            JOIN sys.schemas  s  ON o.schema_id  = s.schema_id
            JOIN sys.types    tp ON c.user_type_id = tp.user_type_id
            LEFT JOIN sys.default_constraints dc
                   ON c.default_object_id = dc.object_id
            LEFT JOIN sys.extended_properties ep
                   ON ep.major_id = c.object_id
                  AND ep.minor_id = c.column_id
                  AND ep.name = 'MS_Description'
                  AND ep.class = 1
            WHERE o.name = @table
              AND o.type IN ('U','V')
              AND (@schema IS NULL OR s.name = @schema)
            ORDER BY c.column_id;
            """;
        cmdCols.Parameters.AddWithValue("@table", table);
        cmdCols.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        var columns = QueryToJson(cmdCols);

        // Chaves primárias
        using var cmdPk = conn.CreateCommand();
        cmdPk.CommandText = """
            SELECT c.name AS column_name,
                   ic.key_ordinal,
                   ic.is_descending_key
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c        ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            JOIN sys.objects o        ON i.object_id = o.object_id
            JOIN sys.schemas s        ON o.schema_id = s.schema_id
            WHERE i.is_primary_key = 1
              AND o.name = @table
              AND (@schema IS NULL OR s.name = @schema)
            ORDER BY ic.key_ordinal;
            """;
        cmdPk.Parameters.AddWithValue("@table", table);
        cmdPk.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        var primaryKeys = QueryToJson(cmdPk);

        // Chaves estrangeiras
        using var cmdFk = conn.CreateCommand();
        cmdFk.CommandText = """
            SELECT fk.name                              AS fk_name,
                   c_from.name                          AS column_name,
                   s_to.name                            AS referenced_schema,
                   o_to.name                            AS referenced_table,
                   c_to.name                            AS referenced_column,
                   fk.delete_referential_action_desc    AS on_delete,
                   fk.update_referential_action_desc    AS on_update
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            JOIN sys.objects  o_from ON fk.parent_object_id      = o_from.object_id
            JOIN sys.schemas  s_from ON o_from.schema_id         = s_from.schema_id
            JOIN sys.columns  c_from ON fkc.parent_object_id     = c_from.object_id
                                    AND fkc.parent_column_id     = c_from.column_id
            JOIN sys.objects  o_to   ON fkc.referenced_object_id = o_to.object_id
            JOIN sys.schemas  s_to   ON o_to.schema_id           = s_to.schema_id
            JOIN sys.columns  c_to   ON fkc.referenced_object_id = c_to.object_id
                                    AND fkc.referenced_column_id = c_to.column_id
            WHERE o_from.name = @table
              AND (@schema IS NULL OR s_from.name = @schema)
            ORDER BY fk.name, fkc.constraint_column_id;
            """;
        cmdFk.Parameters.AddWithValue("@table", table);
        cmdFk.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        var foreignKeys = QueryToJson(cmdFk);

        // Índices
        using var cmdIdx = conn.CreateCommand();
        cmdIdx.CommandText = """
            SELECT i.name                   AS index_name,
                   i.type_desc              AS index_type,
                   i.is_unique,
                   STRING_AGG(c.name, ', ')
                       WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c        ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            JOIN sys.objects o        ON i.object_id = o.object_id
            JOIN sys.schemas s        ON o.schema_id = s.schema_id
            WHERE i.is_primary_key = 0
              AND i.type > 0
              AND ic.is_included_column = 0
              AND o.name = @table
              AND (@schema IS NULL OR s.name = @schema)
            GROUP BY i.name, i.type_desc, i.is_unique
            ORDER BY i.name;
            """;
        cmdIdx.Parameters.AddWithValue("@table", table);
        cmdIdx.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        var indexes = QueryToJson(cmdIdx);

        return JsonSerializer.Serialize(new
        {
            columns   = JsonDocument.Parse(columns).RootElement,
            primary_keys = JsonDocument.Parse(primaryKeys).RootElement,
            foreign_keys = JsonDocument.Parse(foreignKeys).RootElement,
            indexes   = JsonDocument.Parse(indexes).RootElement
        });
    }

    #endregion

    #region Object Definitions

    private string GetObjectDefinition(string objectName, string? schema, string objectTypeFilter)
    {
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT s.name        AS schema_name,
                   o.name        AS object_name,
                   o.type_desc   AS object_type,
                   m.definition  AS definition,
                   o.create_date,
                   o.modify_date
            FROM sys.sql_modules m
            JOIN sys.objects o ON m.object_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE o.name = @name
              AND (@schema IS NULL OR s.name = @schema)
              AND (@typeFilter IS NULL OR o.type = @typeFilter);
            """;
        cmd.Parameters.AddWithValue("@name", objectName);
        cmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@typeFilter", (object?)objectTypeFilter ?? DBNull.Value);
        return QueryToJson(cmd);
    }

    public string GetViewDefinition(string view, string? schema)
    {
        // Colunas da view + definição DDL
        var definition = GetObjectDefinition(view, schema, "V");

        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT c.column_id,
                   c.name       AS column_name,
                   tp.name      AS data_type,
                   c.is_nullable
            FROM sys.columns c
            JOIN sys.objects  o  ON c.object_id   = o.object_id
            JOIN sys.schemas  s  ON o.schema_id   = s.schema_id
            JOIN sys.types    tp ON c.user_type_id = tp.user_type_id
            WHERE o.name = @view
              AND o.type = 'V'
              AND (@schema IS NULL OR s.name = @schema)
            ORDER BY c.column_id;
            """;
        cmd.Parameters.AddWithValue("@view", view);
        cmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        var columns = QueryToJson(cmd);

        return JsonSerializer.Serialize(new
        {
            definition = JsonDocument.Parse(definition).RootElement,
            columns    = JsonDocument.Parse(columns).RootElement
        });
    }

    public string GetProcedureDefinition(string procedure, string? schema)
        => GetObjectDefinition(procedure, schema, "P");

    public string GetFunctionDefinition(string function, string? schema)
    {
        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        // Functions can be FN (scalar), IF (inline table) or TF (multi-statement table)
        cmd.CommandText = """
            SELECT s.name        AS schema_name,
                   o.name        AS object_name,
                   o.type_desc   AS object_type,
                   m.definition  AS definition,
                   o.create_date,
                   o.modify_date
            FROM sys.sql_modules m
            JOIN sys.objects o ON m.object_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE o.name = @name
              AND (@schema IS NULL OR s.name = @schema)
              AND o.type IN ('FN','IF','TF');
            """;
        cmd.Parameters.AddWithValue("@name", function);
        cmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        return QueryToJson(cmd);
    }

    #endregion

    /// <summary>
    /// Executes a SELECT against a table or view.
    /// Builds the SQL internally to prevent injection.
    /// </summary>
    public string QueryTable(
        string table,
        string? schema,
        string? columns,
        string? where,
        string? orderBy,
        int top,
        int skip,
        int maxRows)
    {
        top = Math.Min(top, maxRows);

        // Validates that table and schema are simple identifiers (no quotes, dots, etc.)
        ValidateIdentifier(table, "table");
        if (schema is not null) ValidateIdentifier(schema, "schema");

        var quotedTable = schema is not null
            ? $"[{schema}].[{table}]"
            : $"[{table}]";

        // Columns: validate each one individually
        string colList;
        if (string.IsNullOrWhiteSpace(columns) || columns.Trim() == "*")
        {
            colList = "*";
        }
        else
        {
            var cols = columns.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            foreach (var col in cols) ValidateIdentifier(col, "column");
            colList = string.Join(", ", cols.Select(c => $"[{c}]"));
        }

        // ORDER BY is required when using OFFSET
        var effectiveOrderBy = orderBy;
        if (skip > 0 && string.IsNullOrWhiteSpace(effectiveOrderBy))
            effectiveOrderBy = "(SELECT NULL)";  // arbitrary but valid order

        var sb = new StringBuilder();

        // TOP and OFFSET/FETCH cannot coexist: use OFFSET when paginating
        if (skip > 0)
            sb.Append($"SELECT {colList} FROM {quotedTable}");
        else
            sb.Append($"SELECT TOP ({top}) {colList} FROM {quotedTable}");

        // WHERE is accepted as free text — controlled risk since the server is internal
        // and no DML is possible on this path
        if (!string.IsNullOrWhiteSpace(where))
            sb.Append($" WHERE {where}");

        if (!string.IsNullOrWhiteSpace(effectiveOrderBy))
            sb.Append($" ORDER BY {effectiveOrderBy}");

        if (skip > 0)
            sb.Append($" OFFSET {skip} ROWS FETCH NEXT {top} ROWS ONLY");

        using var conn = OpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sb.ToString();
        cmd.CommandTimeout = 30;
        return QueryToJson(cmd);
    }

    private static readonly System.Text.RegularExpressions.Regex IdentifierPattern =
        new(@"^[a-zA-Z_][a-zA-Z0-9_$#]*$",
            System.Text.RegularExpressions.RegexOptions.Compiled);

    private static void ValidateIdentifier(string value, string paramName)
    {
        if (!IdentifierPattern.IsMatch(value))
            throw new ArgumentException(
                $"Invalid value for '{paramName}': '{value}'. " +
                "Use only letters, digits and underscores.");
    }
}
