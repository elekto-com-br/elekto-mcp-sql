// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

using System.Data;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;

namespace Elekto.Mcp.Sql.Data;

/// <summary>
/// Encapsulates all SQL Server 2022+ queries.
/// Uses sys.* views instead of INFORMATION_SCHEMA for richer and more precise metadata.
/// </summary>
public sealed class SchemaReader
{
    private readonly string _connectionString;
    private readonly int _defaultTimeoutSeconds;

    private static readonly Regex IdentifierPattern =
        new(@"^[a-zA-Z_][a-zA-Z0-9_$#]*$", RegexOptions.Compiled);

    private static readonly Regex AggregatePattern =
        new(
            @"^(?<func>COUNT|SUM|AVG|MIN|MAX)\s*\(\s*(?<col>\*|[a-zA-Z_][a-zA-Z0-9_$#]*)\s*\)\s*(?:AS\s+(?<alias>[a-zA-Z_][a-zA-Z0-9_$#]*))?$",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

    public SchemaReader(string connectionString, int defaultTimeoutSeconds = 30)
    {
        _connectionString = connectionString;
        _defaultTimeoutSeconds = defaultTimeoutSeconds > 0 ? defaultTimeoutSeconds : 30;
    }

    private SqlConnection OpenConnection()
    {
        var conn = new SqlConnection(_connectionString);
        conn.Open();
        return conn;
    }

    private SqlCommand CreateCommand(SqlConnection conn, string sql)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = _defaultTimeoutSeconds;
        return cmd;
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
        using var cmd = CreateCommand(conn, """
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
            """);

        using var reader = cmd.ExecuteReader();
        if (!reader.Read()) return "{}";

        var row = new Dictionary<string, object?>();
        for (int i = 0; i < reader.FieldCount; i++)
            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);

        return JsonSerializer.Serialize(row);
    }

    public string GetSchemaSummary(string? schema)
    {
        using var conn = OpenConnection();
        using var cmd = CreateCommand(conn, """
            WITH table_stats AS (
                SELECT
                    t.schema_id,
                    SUM(COALESCE(p.rows, 0)) AS row_count_approx,
                    SUM(CASE WHEN au.type_desc IN ('IN_ROW_DATA','LOB_DATA','ROW_OVERFLOW_DATA') THEN au.total_pages ELSE 0 END) AS data_pages,
                    SUM(CASE WHEN au.type_desc = 'INDEX_PAGE' THEN au.total_pages ELSE 0 END) AS index_pages
                FROM sys.tables t
                LEFT JOIN sys.indexes i ON i.object_id = t.object_id
                LEFT JOIN sys.partitions p ON p.object_id = i.object_id AND p.index_id = i.index_id
                LEFT JOIN sys.allocation_units au ON au.container_id = p.partition_id
                GROUP BY t.schema_id
            )
            SELECT
                s.name AS schema_name,
                (SELECT COUNT(*) FROM sys.tables t WHERE t.schema_id = s.schema_id) AS table_count,
                (SELECT COUNT(*) FROM sys.views v WHERE v.schema_id = s.schema_id) AS view_count,
                (SELECT COUNT(*) FROM sys.procedures p WHERE p.schema_id = s.schema_id) AS procedure_count,
                (SELECT COUNT(*) FROM sys.objects o WHERE o.schema_id = s.schema_id AND o.type IN ('FN','IF','TF')) AS function_count,
                COALESCE(ts.row_count_approx, 0) AS row_count_approx,
                CAST(COALESCE(ts.data_pages, 0) * 8.0 / 1024 AS DECIMAL(18,2)) AS data_mb,
                CAST(COALESCE(ts.index_pages, 0) * 8.0 / 1024 AS DECIMAL(18,2)) AS index_mb,
                CAST((COALESCE(ts.data_pages, 0) + COALESCE(ts.index_pages, 0)) * 8.0 / 1024 AS DECIMAL(18,2)) AS total_mb
            FROM sys.schemas s
            LEFT JOIN table_stats ts ON ts.schema_id = s.schema_id
            WHERE s.name NOT IN ('sys','guest','INFORMATION_SCHEMA',
                                 'db_owner','db_accessadmin','db_securityadmin',
                                 'db_ddladmin','db_backupoperator','db_datareader',
                                 'db_datawriter','db_denydatareader','db_denydatawriter')
              AND (@schema IS NULL OR s.name = @schema)
            ORDER BY table_count DESC, total_mb DESC, s.name;
            """);
        cmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        return QueryToJson(cmd);
    }

    public string ListSchemas()
    {
        using var conn = OpenConnection();
        using var cmd = CreateCommand(conn, """
            SELECT s.name AS schema_name,
                   p.name AS owner
            FROM sys.schemas s
            JOIN sys.database_principals p ON s.principal_id = p.principal_id
            WHERE s.name NOT IN ('sys','guest','INFORMATION_SCHEMA',
                                 'db_owner','db_accessadmin','db_securityadmin',
                                 'db_ddladmin','db_backupoperator','db_datareader',
                                 'db_datawriter','db_denydatareader','db_denydatawriter')
            ORDER BY s.name;
            """);
        return QueryToJson(cmd);
    }

    public string ListTables(string? schema)
    {
        using var conn = OpenConnection();
        using var cmd = CreateCommand(conn, """
            SELECT
                s.name AS schema_name,
                t.name AS table_name,
                SUM(CASE WHEN p.index_id IN (0,1) THEN p.rows ELSE 0 END) AS row_count_approx,
                t.create_date,
                t.modify_date,
                CAST(SUM(CASE WHEN au.type_desc IN ('IN_ROW_DATA','LOB_DATA','ROW_OVERFLOW_DATA') THEN au.total_pages ELSE 0 END) * 8.0 / 1024 AS DECIMAL(18,2)) AS data_mb,
                CAST(SUM(CASE WHEN au.type_desc = 'INDEX_PAGE' THEN au.total_pages ELSE 0 END) * 8.0 / 1024 AS DECIMAL(18,2)) AS index_mb
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            LEFT JOIN sys.partitions p ON p.object_id = t.object_id
            LEFT JOIN sys.allocation_units au ON au.container_id = p.partition_id
            WHERE (@schema IS NULL OR s.name = @schema)
            GROUP BY s.name, t.name, t.create_date, t.modify_date
            ORDER BY s.name, t.name;
            """);
        cmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        return QueryToJson(cmd);
    }

    public string ListViews(string? schema)
    {
        using var conn = OpenConnection();
        using var cmd = CreateCommand(conn, """
            SELECT s.name AS schema_name,
                   v.name AS view_name,
                   v.is_replicated,
                   v.with_check_option
            FROM sys.views v
            JOIN sys.schemas s ON v.schema_id = s.schema_id
            WHERE (@schema IS NULL OR s.name = @schema)
            ORDER BY s.name, v.name;
            """);
        cmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        return QueryToJson(cmd);
    }

    public string ListProcedures(string? schema)
    {
        using var conn = OpenConnection();
        using var cmd = CreateCommand(conn, """
            SELECT
                s.name AS schema_name,
                p.name AS procedure_name,
                p.create_date,
                p.modify_date,
                COALESCE(LEN(m.definition) - LEN(REPLACE(m.definition, CHAR(10), '')) + 1, 0) AS line_count,
                COALESCE((LEN(LOWER(m.definition)) - LEN(REPLACE(LOWER(m.definition), ' join ', ''))) / 6, 0) AS join_count,
                (
                    SELECT COUNT(DISTINCT sed.referenced_id)
                    FROM sys.sql_expression_dependencies sed
                    WHERE sed.referencing_id = p.object_id
                      AND sed.referenced_id IS NOT NULL
                ) AS referenced_object_count
            FROM sys.procedures p
            JOIN sys.schemas s ON p.schema_id = s.schema_id
            LEFT JOIN sys.sql_modules m ON m.object_id = p.object_id
            WHERE (@schema IS NULL OR s.name = @schema)
            ORDER BY s.name, p.name;
            """);
        cmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        return QueryToJson(cmd);
    }

    public string ListFunctions(string? schema)
    {
        using var conn = OpenConnection();
        using var cmd = CreateCommand(conn, """
            SELECT
                s.name AS schema_name,
                o.name AS function_name,
                CASE o.type
                    WHEN 'FN' THEN 'scalar'
                    WHEN 'IF' THEN 'inline_table'
                    WHEN 'TF' THEN 'multi_statement_table'
                END AS function_type,
                o.create_date,
                o.modify_date,
                COALESCE(LEN(m.definition) - LEN(REPLACE(m.definition, CHAR(10), '')) + 1, 0) AS line_count,
                COALESCE((LEN(LOWER(m.definition)) - LEN(REPLACE(LOWER(m.definition), ' join ', ''))) / 6, 0) AS join_count,
                (
                    SELECT COUNT(DISTINCT sed.referenced_id)
                    FROM sys.sql_expression_dependencies sed
                    WHERE sed.referencing_id = o.object_id
                      AND sed.referenced_id IS NOT NULL
                ) AS referenced_object_count
            FROM sys.objects o
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            LEFT JOIN sys.sql_modules m ON m.object_id = o.object_id
            WHERE o.type IN ('FN','IF','TF')
              AND (@schema IS NULL OR s.name = @schema)
            ORDER BY s.name, o.name;
            """);
        cmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        return QueryToJson(cmd);
    }

    #endregion

    #region Table Schema

    public string GetTableSchema(string table, string? schema)
    {
        ValidateIdentifier(table, nameof(table));
        if (!string.IsNullOrWhiteSpace(schema)) ValidateIdentifier(schema, nameof(schema));

        using var conn = OpenConnection();

        using var cmdCols = CreateCommand(conn, """
            SELECT c.column_id,
                   c.name                                          AS column_name,
                   tp.name                                         AS data_type,
                   c.max_length,
                   c.precision,
                   c.scale,
                   c.collation_name,
                   c.is_nullable,
                   c.is_identity,
                   c.is_computed,
                   cc.definition                                   AS computed_definition,
                   dc.definition                                   AS default_value,
                   ep.value                                        AS description
            FROM sys.columns c
            JOIN sys.objects  o  ON c.object_id  = o.object_id
            JOIN sys.schemas  s  ON o.schema_id  = s.schema_id
            JOIN sys.types    tp ON c.user_type_id = tp.user_type_id
            LEFT JOIN sys.default_constraints dc
                   ON c.default_object_id = dc.object_id
            LEFT JOIN sys.computed_columns cc
                   ON c.object_id = cc.object_id AND c.column_id = cc.column_id
            LEFT JOIN sys.extended_properties ep
                   ON ep.major_id = c.object_id
                  AND ep.minor_id = c.column_id
                  AND ep.name = 'MS_Description'
                  AND ep.class = 1
            WHERE o.name = @table
              AND o.type IN ('U','V')
              AND (@schema IS NULL OR s.name = @schema)
            ORDER BY c.column_id;
            """);
        cmdCols.Parameters.AddWithValue("@table", table);
        cmdCols.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        var columns = QueryToJson(cmdCols);

        using var cmdPk = CreateCommand(conn, """
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
            """);
        cmdPk.Parameters.AddWithValue("@table", table);
        cmdPk.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        var primaryKeys = QueryToJson(cmdPk);

        using var cmdFk = CreateCommand(conn, """
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
            """);
        cmdFk.Parameters.AddWithValue("@table", table);
        cmdFk.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        var foreignKeys = QueryToJson(cmdFk);

        using var cmdIdx = CreateCommand(conn, """
            SELECT i.name                   AS index_name,
                   i.type_desc              AS index_type,
                   i.is_unique,
                   STRING_AGG(CASE WHEN ic.is_included_column = 0 THEN c.name END, ', ') AS key_columns,
                   STRING_AGG(CASE WHEN ic.is_included_column = 1 THEN c.name END, ', ') AS included_columns,
                   i.filter_definition
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c        ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            JOIN sys.objects o        ON i.object_id = o.object_id
            JOIN sys.schemas s        ON o.schema_id = s.schema_id
            WHERE i.type > 0
              AND o.name = @table
              AND (@schema IS NULL OR s.name = @schema)
            GROUP BY i.name, i.type_desc, i.is_unique, i.filter_definition
            ORDER BY i.name;
            """);
        cmdIdx.Parameters.AddWithValue("@table", table);
        cmdIdx.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        var indexes = QueryToJson(cmdIdx);

        using var cmdChecks = CreateCommand(conn, """
            SELECT cc.name AS check_name,
                   cc.definition
            FROM sys.check_constraints cc
            JOIN sys.objects o ON cc.parent_object_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE o.name = @table
              AND (@schema IS NULL OR s.name = @schema)
            ORDER BY cc.name;
            """);
        cmdChecks.Parameters.AddWithValue("@table", table);
        cmdChecks.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        var checks = QueryToJson(cmdChecks);

        using var cmdUnique = CreateCommand(conn, """
            SELECT kc.name AS unique_constraint_name,
                   STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
            FROM sys.key_constraints kc
            JOIN sys.objects o ON kc.parent_object_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            JOIN sys.index_columns ic ON ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id
            JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE kc.type = 'UQ'
              AND o.name = @table
              AND (@schema IS NULL OR s.name = @schema)
            GROUP BY kc.name
            ORDER BY kc.name;
            """);
        cmdUnique.Parameters.AddWithValue("@table", table);
        cmdUnique.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        var uniques = QueryToJson(cmdUnique);

        return JsonSerializer.Serialize(new
        {
            columns = JsonDocument.Parse(columns).RootElement,
            primary_keys = JsonDocument.Parse(primaryKeys).RootElement,
            foreign_keys = JsonDocument.Parse(foreignKeys).RootElement,
            indexes = JsonDocument.Parse(indexes).RootElement,
            checks = JsonDocument.Parse(checks).RootElement,
            unique_constraints = JsonDocument.Parse(uniques).RootElement
        });
    }

    #endregion

    #region Object Definitions

    private string GetObjectDefinition(string objectName, string? schema, string objectTypeFilter)
    {
        using var conn = OpenConnection();
        using var cmd = CreateCommand(conn, """
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
            """);
        cmd.Parameters.AddWithValue("@name", objectName);
        cmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@typeFilter", (object?)objectTypeFilter ?? DBNull.Value);
        return QueryToJson(cmd);
    }

    public string GetViewDefinition(string view, string? schema)
    {
        var definition = GetObjectDefinition(view, schema, "V");

        using var conn = OpenConnection();
        using var cmd = CreateCommand(conn, """
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
            """);
        cmd.Parameters.AddWithValue("@view", view);
        cmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        var columns = QueryToJson(cmd);

        return JsonSerializer.Serialize(new
        {
            definition = JsonDocument.Parse(definition).RootElement,
            columns = JsonDocument.Parse(columns).RootElement
        });
    }

    public string GetProcedureDefinition(string procedure, string? schema)
        => GetObjectDefinition(procedure, schema, "P");

    public string GetFunctionDefinition(string function, string? schema)
    {
        using var conn = OpenConnection();
        using var cmd = CreateCommand(conn, """
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
            """);
        cmd.Parameters.AddWithValue("@name", function);
        cmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        return QueryToJson(cmd);
    }

    #endregion

    #region Advanced exploration

    public string GetDependencyGraph(string? schema)
    {
        using var conn = OpenConnection();
        using var cmd = CreateCommand(conn, """
            SELECT
                dep.dependency_kind,
                dep.from_schema,
                dep.from_object,
                dep.from_type,
                dep.to_schema,
                dep.to_object,
                dep.to_type
            FROM (
                SELECT
                    'FOREIGN_KEY' AS dependency_kind,
                    sf.name AS from_schema,
                    tf.name AS from_object,
                    'TABLE' AS from_type,
                    st.name AS to_schema,
                    tt.name AS to_object,
                    'TABLE' AS to_type
                FROM sys.foreign_keys fk
                JOIN sys.tables tf ON fk.parent_object_id = tf.object_id
                JOIN sys.schemas sf ON tf.schema_id = sf.schema_id
                JOIN sys.tables tt ON fk.referenced_object_id = tt.object_id
                JOIN sys.schemas st ON tt.schema_id = st.schema_id
                WHERE (@schema IS NULL OR sf.name = @schema OR st.name = @schema)

                UNION ALL

                SELECT
                    'SQL_EXPRESSION' AS dependency_kind,
                    s1.name AS from_schema,
                    o1.name AS from_object,
                    o1.type_desc AS from_type,
                    s2.name AS to_schema,
                    o2.name AS to_object,
                    o2.type_desc AS to_type
                FROM sys.sql_expression_dependencies d
                JOIN sys.objects o1 ON o1.object_id = d.referencing_id
                JOIN sys.schemas s1 ON s1.schema_id = o1.schema_id
                LEFT JOIN sys.objects o2 ON o2.object_id = d.referenced_id
                LEFT JOIN sys.schemas s2 ON s2.schema_id = o2.schema_id
                WHERE d.referenced_id IS NOT NULL
                  AND (@schema IS NULL OR s1.name = @schema OR s2.name = @schema)
            ) dep
            ORDER BY dep.from_schema, dep.from_object, dep.to_schema, dep.to_object;
            """);
        cmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        return QueryToJson(cmd);
    }

    public string GetTableUsage(string table, string? schema)
    {
        ValidateIdentifier(table, nameof(table));
        var effectiveSchema = string.IsNullOrWhiteSpace(schema) ? "dbo" : schema;
        ValidateIdentifier(effectiveSchema, nameof(schema));

        using var conn = OpenConnection();

        using var cmdFk = CreateCommand(conn, """
            SELECT
                'FOREIGN_KEY' AS usage_type,
                ss.name AS referencing_schema,
                st.name AS referencing_object,
                'TABLE' AS referencing_type,
                fk.name AS details
            FROM sys.foreign_keys fk
            JOIN sys.tables st ON st.object_id = fk.parent_object_id
            JOIN sys.schemas ss ON ss.schema_id = st.schema_id
            WHERE fk.referenced_object_id = OBJECT_ID(QUOTENAME(@schema) + '.' + QUOTENAME(@table));
            """);
        cmdFk.Parameters.AddWithValue("@schema", effectiveSchema);
        cmdFk.Parameters.AddWithValue("@table", table);
        var fkUsage = QueryToJson(cmdFk);

        using var cmdSql = CreateCommand(conn, """
            SELECT
                'SQL_REFERENCE' AS usage_type,
                s.name AS referencing_schema,
                o.name AS referencing_object,
                o.type_desc AS referencing_type,
                NULL AS details
            FROM sys.sql_expression_dependencies d
            JOIN sys.objects o ON o.object_id = d.referencing_id
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE d.referenced_id = OBJECT_ID(QUOTENAME(@schema) + '.' + QUOTENAME(@table))
            ORDER BY s.name, o.name;
            """);
        cmdSql.Parameters.AddWithValue("@schema", effectiveSchema);
        cmdSql.Parameters.AddWithValue("@table", table);
        var sqlUsage = QueryToJson(cmdSql);

        return JsonSerializer.Serialize(new
        {
            table = new { schema = effectiveSchema, name = table },
            foreign_key_usage = JsonDocument.Parse(fkUsage).RootElement,
            sql_module_usage = JsonDocument.Parse(sqlUsage).RootElement
        });
    }

    public string GetDataProfile(string table, string? schema, string? columns, int topValues)
    {
        ValidateIdentifier(table, nameof(table));
        var effectiveSchema = string.IsNullOrWhiteSpace(schema) ? "dbo" : schema;
        ValidateIdentifier(effectiveSchema, nameof(schema));

        if (topValues <= 0)
            throw new ArgumentException("'top_values' must be greater than zero.", nameof(topValues));

        var selectedColumns = ParseIdentifierList(columns);

        using var conn = OpenConnection();

        var metadata = new List<(string Name, string Type)>();
        using (var cmdColumns = CreateCommand(conn, """
            SELECT c.name AS column_name, tp.name AS data_type
            FROM sys.columns c
            JOIN sys.types tp ON tp.user_type_id = c.user_type_id
            WHERE c.object_id = OBJECT_ID(QUOTENAME(@schema) + '.' + QUOTENAME(@table))
            ORDER BY c.column_id;
            """))
        {
            cmdColumns.Parameters.AddWithValue("@schema", effectiveSchema);
            cmdColumns.Parameters.AddWithValue("@table", table);

            using var reader = cmdColumns.ExecuteReader();
            while (reader.Read())
            {
                var name = reader.GetString(0);
                if (selectedColumns.Count == 0 || selectedColumns.Contains(name, StringComparer.OrdinalIgnoreCase))
                    metadata.Add((name, reader.GetString(1)));
            }
        }

        var profile = new List<Dictionary<string, object?>>();

        foreach (var (columnName, dataType) in metadata)
        {
            var quotedTable = $"[{effectiveSchema}].[{table}]";
            var quotedColumn = $"[{columnName}]";

            long totalRows;
            long nullCount;
            long distinctCount;
            string? minValue;
            string? maxValue;

            using (var statsCmd = CreateCommand(conn, $"""
                SELECT
                    COUNT_BIG(1) AS total_rows,
                    SUM(CASE WHEN {quotedColumn} IS NULL THEN 1 ELSE 0 END) AS null_count,
                    COUNT_BIG(DISTINCT {quotedColumn}) AS distinct_count,
                    MIN(TRY_CONVERT(NVARCHAR(4000), {quotedColumn})) AS min_value,
                    MAX(TRY_CONVERT(NVARCHAR(4000), {quotedColumn})) AS max_value
                FROM {quotedTable};
                """))
            using (var statsReader = statsCmd.ExecuteReader())
            {
                if (!statsReader.Read())
                    continue;

                totalRows = statsReader.IsDBNull(0) ? 0L : Convert.ToInt64(statsReader.GetValue(0), CultureInfo.InvariantCulture);
                nullCount = statsReader.IsDBNull(1) ? 0L : Convert.ToInt64(statsReader.GetValue(1), CultureInfo.InvariantCulture);
                distinctCount = statsReader.IsDBNull(2) ? 0L : Convert.ToInt64(statsReader.GetValue(2), CultureInfo.InvariantCulture);
                minValue = statsReader.IsDBNull(3) ? null : statsReader.GetString(3);
                maxValue = statsReader.IsDBNull(4) ? null : statsReader.GetString(4);
            }

            var topValuesRows = new List<Dictionary<string, object?>>();
            using (var topCmd = CreateCommand(conn, $"""
                SELECT TOP ({topValues})
                    TRY_CONVERT(NVARCHAR(4000), {quotedColumn}) AS value,
                    COUNT_BIG(1) AS frequency
                FROM {quotedTable}
                WHERE {quotedColumn} IS NOT NULL
                GROUP BY {quotedColumn}
                ORDER BY COUNT_BIG(1) DESC;
                """))
            {
                using var topReader = topCmd.ExecuteReader();
                while (topReader.Read())
                {
                    topValuesRows.Add(new Dictionary<string, object?>
                    {
                        ["value"] = topReader.IsDBNull(0) ? null : topReader.GetString(0),
                        ["frequency"] = topReader.IsDBNull(1) ? 0L : Convert.ToInt64(topReader.GetValue(1), CultureInfo.InvariantCulture)
                    });
                }
            }

            profile.Add(new Dictionary<string, object?>
            {
                ["column_name"] = columnName,
                ["data_type"] = dataType,
                ["total_rows"] = totalRows,
                ["null_count"] = nullCount,
                ["null_ratio"] = totalRows == 0 ? 0m : Math.Round((decimal)nullCount / totalRows, 6),
                ["distinct_count"] = distinctCount,
                ["min_value"] = minValue,
                ["max_value"] = maxValue,
                ["top_values"] = topValuesRows
            });
        }

        return JsonSerializer.Serialize(new
        {
            table = new { schema = effectiveSchema, name = table },
            columns = profile
        });
    }

    public string GetIndexHealth(string? schema)
    {
        using var conn = OpenConnection();

        using var duplicateCmd = CreateCommand(conn, """
            WITH idx AS (
                SELECT
                    s.name AS schema_name,
                    t.name AS table_name,
                    i.name AS index_name,
                    i.index_id,
                    STRING_AGG(CASE WHEN ic.is_included_column = 0 THEN c.name END, ',') AS key_cols,
                    STRING_AGG(CASE WHEN ic.is_included_column = 1 THEN c.name END, ',') AS include_cols
                FROM sys.tables t
                JOIN sys.schemas s ON s.schema_id = t.schema_id
                JOIN sys.indexes i ON i.object_id = t.object_id
                JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                WHERE i.type > 0
                  AND i.is_primary_key = 0
                  AND i.is_unique_constraint = 0
                  AND (@schema IS NULL OR s.name = @schema)
                GROUP BY s.name, t.name, i.name, i.index_id
            )
            SELECT
                a.schema_name,
                a.table_name,
                a.index_name AS index_a,
                b.index_name AS index_b,
                a.key_cols,
                a.include_cols
            FROM idx a
            JOIN idx b
              ON a.schema_name = b.schema_name
             AND a.table_name = b.table_name
             AND a.index_id < b.index_id
             AND ISNULL(a.key_cols, '') = ISNULL(b.key_cols, '')
             AND ISNULL(a.include_cols, '') = ISNULL(b.include_cols, '')
            ORDER BY a.schema_name, a.table_name, a.index_name;
            """);
        duplicateCmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        var duplicateIndexes = QueryToJson(duplicateCmd);

        using var unusedCmd = CreateCommand(conn, """
            SELECT
                s.name AS schema_name,
                t.name AS table_name,
                i.name AS index_name,
                COALESCE(us.user_seeks, 0) AS user_seeks,
                COALESCE(us.user_scans, 0) AS user_scans,
                COALESCE(us.user_lookups, 0) AS user_lookups,
                COALESCE(us.user_updates, 0) AS user_updates
            FROM sys.indexes i
            JOIN sys.tables t ON t.object_id = i.object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            LEFT JOIN sys.dm_db_index_usage_stats us
              ON us.database_id = DB_ID()
             AND us.object_id = i.object_id
             AND us.index_id = i.index_id
            WHERE i.type > 0
              AND i.is_primary_key = 0
              AND i.is_unique_constraint = 0
              AND (@schema IS NULL OR s.name = @schema)
              AND COALESCE(us.user_seeks, 0) = 0
              AND COALESCE(us.user_scans, 0) = 0
              AND COALESCE(us.user_lookups, 0) = 0
            ORDER BY s.name, t.name, i.name;
            """);
        unusedCmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        var unusedIndexes = QueryToJson(unusedCmd);

        using var missingCmd = CreateCommand(conn, """
            SELECT
                OBJECT_SCHEMA_NAME(mid.object_id) AS schema_name,
                OBJECT_NAME(mid.object_id) AS table_name,
                migs.user_seeks,
                migs.user_scans,
                migs.avg_total_user_cost,
                migs.avg_user_impact,
                mid.equality_columns,
                mid.inequality_columns,
                mid.included_columns
            FROM sys.dm_db_missing_index_group_stats migs
            JOIN sys.dm_db_missing_index_groups mig ON migs.group_handle = mig.index_group_handle
            JOIN sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
            WHERE mid.database_id = DB_ID()
              AND (@schema IS NULL OR OBJECT_SCHEMA_NAME(mid.object_id) = @schema)
            ORDER BY (migs.avg_user_impact * (migs.user_seeks + migs.user_scans)) DESC;
            """);
        missingCmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
        var missingIndexes = QueryToJson(missingCmd);

        return JsonSerializer.Serialize(new
        {
            duplicate_indexes = JsonDocument.Parse(duplicateIndexes).RootElement,
            unused_indexes = JsonDocument.Parse(unusedIndexes).RootElement,
            missing_index_suggestions = JsonDocument.Parse(missingIndexes).RootElement
        });
    }

    public string GenerateDependencyDot(string? schema)
    {
        using var conn = OpenConnection();

        var edges = new List<DependencyEdge>();
        using (var edgeCmd = CreateCommand(conn, """
            SELECT
                dep.dependency_kind,
                dep.from_schema,
                dep.from_object,
                dep.from_type,
                dep.to_schema,
                dep.to_object,
                dep.to_type
            FROM (
                SELECT
                    'FOREIGN_KEY' AS dependency_kind,
                    sf.name AS from_schema,
                    tf.name AS from_object,
                    'TABLE' AS from_type,
                    st.name AS to_schema,
                    tt.name AS to_object,
                    'TABLE' AS to_type
                FROM sys.foreign_keys fk
                JOIN sys.tables tf ON fk.parent_object_id = tf.object_id
                JOIN sys.schemas sf ON tf.schema_id = sf.schema_id
                JOIN sys.tables tt ON fk.referenced_object_id = tt.object_id
                JOIN sys.schemas st ON tt.schema_id = st.schema_id
                WHERE (@schema IS NULL OR sf.name = @schema OR st.name = @schema)

                UNION ALL

                SELECT
                    'SQL_EXPRESSION' AS dependency_kind,
                    s1.name AS from_schema,
                    o1.name AS from_object,
                    o1.type_desc AS from_type,
                    s2.name AS to_schema,
                    o2.name AS to_object,
                    o2.type_desc AS to_type
                FROM sys.sql_expression_dependencies d
                JOIN sys.objects o1 ON o1.object_id = d.referencing_id
                JOIN sys.schemas s1 ON s1.schema_id = o1.schema_id
                LEFT JOIN sys.objects o2 ON o2.object_id = d.referenced_id
                LEFT JOIN sys.schemas s2 ON s2.schema_id = o2.schema_id
                WHERE d.referenced_id IS NOT NULL
                  AND (@schema IS NULL OR s1.name = @schema OR s2.name = @schema)
            ) dep
            ORDER BY dep.from_schema, dep.from_object, dep.to_schema, dep.to_object;
            """))
        {
            edgeCmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
            using var reader = edgeCmd.ExecuteReader();
            while (reader.Read())
            {
                var edge = new DependencyEdge(
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    NormalizeNodeKind(reader.GetString(3)),
                    reader.GetString(4),
                    reader.GetString(5),
                    NormalizeNodeKind(reader.GetString(6)));
                edges.Add(edge);
            }
        }

        var nodes = new Dictionary<string, DependencyNode>(StringComparer.OrdinalIgnoreCase);

        foreach (var edge in edges)
        {
            var fromId = BuildNodeId(edge.FromSchema, edge.FromObject);
            if (!nodes.ContainsKey(fromId))
                nodes[fromId] = new DependencyNode(fromId, edge.FromSchema, edge.FromObject, edge.FromKind);

            var toId = BuildNodeId(edge.ToSchema, edge.ToObject);
            if (!nodes.ContainsKey(toId))
                nodes[toId] = new DependencyNode(toId, edge.ToSchema, edge.ToObject, edge.ToKind);
        }

        using (var nodeCmd = CreateCommand(conn, """
            SELECT
                s.name AS schema_name,
                o.name AS object_name,
                o.type_desc AS object_type
            FROM sys.objects o
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE o.type IN ('U','V','P','FN','IF','TF')
              AND (@schema IS NULL OR s.name = @schema)
            ORDER BY s.name, o.name;
            """))
        {
            nodeCmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
            using var reader = nodeCmd.ExecuteReader();
            while (reader.Read())
            {
                var nodeSchema = reader.GetString(0);
                var nodeObject = reader.GetString(1);
                var nodeKind = NormalizeNodeKind(reader.GetString(2));
                var nodeId = BuildNodeId(nodeSchema, nodeObject);
                if (!nodes.ContainsKey(nodeId))
                    nodes[nodeId] = new DependencyNode(nodeId, nodeSchema, nodeObject, nodeKind);
            }
        }

        var orderedNodes = nodes.Values
            .OrderBy(n => n.Schema, StringComparer.OrdinalIgnoreCase)
            .ThenBy(n => n.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var orderedEdges = edges
            .OrderBy(e => e.FromSchema, StringComparer.OrdinalIgnoreCase)
            .ThenBy(e => e.FromObject, StringComparer.OrdinalIgnoreCase)
            .ThenBy(e => e.ToSchema, StringComparer.OrdinalIgnoreCase)
            .ThenBy(e => e.ToObject, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var sb = new StringBuilder();
        sb.AppendLine("digraph dependencies {");
        sb.AppendLine("  rankdir=LR;");

        foreach (var node in orderedNodes)
        {
            var label = $"{node.Schema}.{node.Name}";
            sb.AppendLine($"  \"{EscapeDot(node.Id)}\" [label=\"{EscapeDot(label)}\", node_kind=\"{node.Kind.ToLowerInvariant()}\", shape=\"{GetDotShape(node.Kind)}\"];");
        }

        foreach (var edge in orderedEdges)
        {
            var fromId = BuildNodeId(edge.FromSchema, edge.FromObject);
            var toId = BuildNodeId(edge.ToSchema, edge.ToObject);
            sb.AppendLine($"  \"{EscapeDot(fromId)}\" -> \"{EscapeDot(toId)}\" [dependency_kind=\"{edge.DependencyKind.ToLowerInvariant()}\"];");
        }

        sb.AppendLine("}");

        return JsonSerializer.Serialize(new
        {
            format = "dot",
            graph_type = "digraph",
            nodes = orderedNodes.Select(n => new
            {
                id = n.Id,
                schema_name = n.Schema,
                object_name = n.Name,
                node_kind = n.Kind
            }),
            edges = orderedEdges.Select(e => new
            {
                dependency_kind = e.DependencyKind,
                from_node_id = BuildNodeId(e.FromSchema, e.FromObject),
                to_node_id = BuildNodeId(e.ToSchema, e.ToObject)
            }),
            dot = sb.ToString()
        });
    }

    #endregion

    /// <summary>
    /// Executes a SELECT against a table or view.
    /// Builds the SQL internally to prevent injection on composed identifiers.
    /// </summary>
    public string QueryTable(
        string table,
        string? schema,
        string? columns,
        string? where,
        string? orderBy,
        int top,
        int skip,
        int maxRows,
        string? groupBy = null,
        string? aggregates = null,
        decimal? samplePercent = null)
    {
        if (top <= 0)
            throw new ArgumentException("'top' must be greater than zero.", nameof(top));

        if (skip < 0)
            throw new ArgumentException("'skip' cannot be negative.", nameof(skip));

        top = Math.Min(top, maxRows);

        ValidateIdentifier(table, nameof(table));
        var effectiveSchema = string.IsNullOrWhiteSpace(schema) ? null : schema;
        if (effectiveSchema is not null) ValidateIdentifier(effectiveSchema, nameof(schema));

        if (samplePercent is not null && (samplePercent <= 0 || samplePercent > 100))
            throw new ArgumentException("'sample_percent' must be between 0.01 and 100.", nameof(samplePercent));

        var quotedTable = effectiveSchema is not null
            ? $"[{effectiveSchema}].[{table}]"
            : $"[{table}]";

        var groupColumns = ParseIdentifierList(groupBy).Select(c => $"[{c}]").ToList();
        var aggregateExpressions = ParseAggregateExpressions(aggregates);

        string selectList;
        if (groupColumns.Count > 0 || aggregateExpressions.Count > 0)
        {
            var parts = new List<string>();
            parts.AddRange(groupColumns);
            parts.AddRange(aggregateExpressions);

            if (parts.Count == 0)
                throw new ArgumentException("At least one column or aggregate must be provided when grouping.");

            selectList = string.Join(", ", parts);
        }
        else
        {
            if (string.IsNullOrWhiteSpace(columns) || columns.Trim() == "*")
            {
                selectList = "*";
            }
            else
            {
                var cols = ParseIdentifierList(columns);
                selectList = string.Join(", ", cols.Select(c => $"[{c}]"));
            }
        }

        var effectiveOrderBy = orderBy;
        if (skip > 0 && string.IsNullOrWhiteSpace(effectiveOrderBy))
            effectiveOrderBy = "(SELECT NULL)";

        var sb = new StringBuilder();

        if (skip > 0)
            sb.Append($"SELECT {selectList} FROM {quotedTable}");
        else
            sb.Append($"SELECT TOP ({top}) {selectList} FROM {quotedTable}");

        var whereParts = new List<string>();
        if (!string.IsNullOrWhiteSpace(where))
            whereParts.Add($"({where})");

        if (samplePercent is not null)
            whereParts.Add($"ABS(CHECKSUM(NEWID())) % 10000 < {Math.Round(samplePercent.Value * 100, 0, MidpointRounding.AwayFromZero).ToString(CultureInfo.InvariantCulture)}");

        if (whereParts.Count > 0)
            sb.Append($" WHERE {string.Join(" AND ", whereParts)}");

        if (groupColumns.Count > 0)
            sb.Append($" GROUP BY {string.Join(", ", groupColumns)}");

        if (!string.IsNullOrWhiteSpace(effectiveOrderBy))
            sb.Append($" ORDER BY {effectiveOrderBy}");

        if (skip > 0)
            sb.Append($" OFFSET {skip} ROWS FETCH NEXT {top} ROWS ONLY");

        using var conn = OpenConnection();
        using var cmd = CreateCommand(conn, sb.ToString());

        try
        {
            return QueryToJson(cmd);
        }
        catch (SqlException ex)
        {
            throw new InvalidOperationException(
                $"Query execution failed for [{effectiveSchema ?? "dbo"}].[{table}]. SQL Server message: {ex.Message}",
                ex);
        }
    }

    private static List<string> ParseIdentifierList(string? list)
    {
        if (string.IsNullOrWhiteSpace(list))
            return new List<string>();

        var identifiers = list
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .ToList();

        if (identifiers.Count == 0)
            throw new ArgumentException("Identifier list cannot be empty.");

        foreach (var identifier in identifiers)
            ValidateIdentifier(identifier, "identifier");

        return identifiers;
    }

    private static List<string> ParseAggregateExpressions(string? aggregates)
    {
        if (string.IsNullOrWhiteSpace(aggregates))
            return new List<string>();

        var expressions = aggregates
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .ToList();

        var result = new List<string>(expressions.Count);
        foreach (var expression in expressions)
        {
            var match = AggregatePattern.Match(expression.Trim());
            if (!match.Success)
                throw new ArgumentException(
                    $"Invalid aggregate expression '{expression}'. Use FUNC(column) [AS alias] with FUNC in COUNT,SUM,AVG,MIN,MAX.");

            var func = match.Groups["func"].Value.ToUpperInvariant();
            var col = match.Groups["col"].Value;
            var alias = match.Groups["alias"].Success ? match.Groups["alias"].Value : null;

            if (col != "*") ValidateIdentifier(col, "aggregate_column");
            if (alias is not null) ValidateIdentifier(alias, "aggregate_alias");

            var colSql = col == "*" ? "*" : $"[{col}]";
            var aliasSql = alias is null ? string.Empty : $" AS [{alias}]";
            result.Add($"{func}({colSql}){aliasSql}");
        }

        return result;
    }

    private static string BuildNodeId(string schema, string name)
        => $"{schema}.{name}";

    private static string NormalizeNodeKind(string objectType)
    {
        if (string.IsNullOrWhiteSpace(objectType))
            return "OTHER";

        return objectType.ToUpperInvariant() switch
        {
            "TABLE" or "USER_TABLE" => "TABLE",
            "VIEW" => "VIEW",
            "SQL_STORED_PROCEDURE" => "PROCEDURE",
            "SQL_SCALAR_FUNCTION" or "SQL_INLINE_TABLE_VALUED_FUNCTION" or "SQL_TABLE_VALUED_FUNCTION" => "FUNCTION",
            _ => "OTHER"
        };
    }

    private static string GetDotShape(string nodeKind)
        => nodeKind.ToUpperInvariant() switch
        {
            "TABLE" => "box",
            "VIEW" => "ellipse",
            "PROCEDURE" => "component",
            "FUNCTION" => "hexagon",
            _ => "oval"
        };

    private static string EscapeDot(string value)
        => value.Replace("\\", "\\\\").Replace("\"", "\\\"");

    private static void ValidateIdentifier(string value, string paramName)
    {
        if (!IdentifierPattern.IsMatch(value))
            throw new ArgumentException(
                $"Invalid value for '{paramName}': '{value}'. " +
                "Use only letters, digits and underscores.");
    }

    private sealed record ColumnDef(string DataType, bool IsNullable, short MaxLength, byte Precision, byte Scale);
    private sealed record DependencyNode(string Id, string Schema, string Name, string Kind);
    private sealed record DependencyEdge(
        string DependencyKind,
        string FromSchema,
        string FromObject,
        string FromKind,
        string ToSchema,
        string ToObject,
        string ToKind);
    public static string CompareSchemas(
        SchemaReader source,
        SchemaReader target,
        string? sourceSchema,
        string? targetSchema)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(target);

        var sourceSnapshot = source.GetSchemaSnapshot(sourceSchema);
        var targetSnapshot = target.GetSchemaSnapshot(targetSchema);

        var sourceTables = sourceSnapshot.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase);
        var targetTables = targetSnapshot.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase);

        var missingInTarget = sourceTables.Except(targetTables, StringComparer.OrdinalIgnoreCase).OrderBy(x => x).ToList();
        var extraInTarget = targetTables.Except(sourceTables, StringComparer.OrdinalIgnoreCase).OrderBy(x => x).ToList();

        var columnDiffs = new List<object>();
        foreach (var tableName in sourceTables.Intersect(targetTables, StringComparer.OrdinalIgnoreCase))
        {
            var srcCols = sourceSnapshot[tableName];
            var tgtCols = targetSnapshot[tableName];

            var srcColNames = srcCols.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase);
            var tgtColNames = tgtCols.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase);

            var missingCols = srcColNames.Except(tgtColNames, StringComparer.OrdinalIgnoreCase).OrderBy(x => x).ToList();
            var extraCols = tgtColNames.Except(srcColNames, StringComparer.OrdinalIgnoreCase).OrderBy(x => x).ToList();

            var changedCols = new List<object>();
            foreach (var col in srcColNames.Intersect(tgtColNames, StringComparer.OrdinalIgnoreCase))
            {
                var s = srcCols[col];
                var t = tgtCols[col];
                if (!string.Equals(s.DataType, t.DataType, StringComparison.OrdinalIgnoreCase) ||
                    s.IsNullable != t.IsNullable ||
                    s.MaxLength != t.MaxLength ||
                    s.Precision != t.Precision ||
                    s.Scale != t.Scale)
                {
                    changedCols.Add(new
                    {
                        column_name = col,
                        source = s,
                        target = t
                    });
                }
            }

            if (missingCols.Count > 0 || extraCols.Count > 0 || changedCols.Count > 0)
            {
                columnDiffs.Add(new
                {
                    table_name = tableName,
                    missing_columns_in_target = missingCols,
                    extra_columns_in_target = extraCols,
                    changed_columns = changedCols
                });
            }
        }

        return JsonSerializer.Serialize(new
        {
            missing_tables_in_target = missingInTarget,
            extra_tables_in_target = extraInTarget,
            table_column_differences = columnDiffs
        });
    }

    private Dictionary<string, Dictionary<string, ColumnDef>> GetSchemaSnapshot(string? schema)
    {
        using var conn = OpenConnection();
        using var cmd = CreateCommand(conn, """
            SELECT
                s.name AS schema_name,
                t.name AS table_name,
                c.name AS column_name,
                tp.name AS data_type,
                c.is_nullable,
                c.max_length,
                c.precision,
                c.scale
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.columns c ON c.object_id = t.object_id
            JOIN sys.types tp ON tp.user_type_id = c.user_type_id
            WHERE (@schema IS NULL OR s.name = @schema)
            ORDER BY s.name, t.name, c.column_id;
            """);
        cmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);

        var snapshot = new Dictionary<string, Dictionary<string, ColumnDef>>(StringComparer.OrdinalIgnoreCase);
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var fullTable = $"{reader.GetString(0)}.{reader.GetString(1)}";
            var column = reader.GetString(2);
            var def = new ColumnDef(
                reader.GetString(3),
                reader.GetBoolean(4),
                reader.GetInt16(5),
                reader.GetByte(6),
                reader.GetByte(7));

            if (!snapshot.TryGetValue(fullTable, out var colMap))
            {
                colMap = new Dictionary<string, ColumnDef>(StringComparer.OrdinalIgnoreCase);
                snapshot[fullTable] = colMap;
            }

            colMap[column] = def;
        }

        return snapshot;
    }
}
