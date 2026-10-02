// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

using System.Text.Json;
using Microsoft.Data.SqlClient;

namespace Elekto.Mcp.Sql.Data;

/// <summary>
/// What the connected login can see, and saying so when it is not everything.
/// </summary>
/// <remarks>
/// <para>
/// SQL Server's catalog views show only the objects the login holds some permission on (metadata
/// visibility), and they filter the rest out without an error. A login in <c>db_datareader</c> alone
/// lists every table, but not a single procedure it cannot execute; <c>sys.sql_modules</c> returns the
/// modules it can see with a NULL definition; <c>sys.sql_expression_dependencies</c> returns only the
/// references of modules whose definition it can see. Every one of those comes back as a normal,
/// well-formed, short answer.
/// </para>
/// <para>
/// A short answer that reads as a complete one is the failure <c>query_table</c>'s <c>truncated</c>
/// flag exists to prevent, and it is worse here, because the caller has no way to find out from the
/// data. So the server checks the permissions that decide visibility and says when they are missing.
/// </para>
/// </remarks>
public sealed partial class SchemaReader
{
    private const string CheckPermissionsHint =
        "Call check_permissions for what this login is missing and the GRANT statements that fix it.";

    private const string NoViewDefinitionNote =
        "The login lacks VIEW DEFINITION on the database. SQL Server then leaves out, without any "
        + "error, every procedure, function and view the login holds no permission on, so their "
        + "listings and counts may be short.";

    private const string NoTableVisibilityNote =
        "The login has neither SELECT nor VIEW DEFINITION on the database, so tables and views it "
        + "holds no permission on are left out of listings and counts.";

    private const string NoSqlDependenciesNote =
        "The login cannot SELECT from sys.sql_expression_dependencies, so references from views, "
        + "procedures and functions are not reported.";

    private const string FilteredSqlDependenciesNote =
        "Without VIEW DEFINITION on the database, sys.sql_expression_dependencies returns only the "
        + "references of modules whose definition the login can see, so module references may be missing.";

    private const string NoServerStateNote =
        "The login cannot read the index usage and missing-index DMVs, which need VIEW SERVER STATE "
        + "(or, from SQL Server 2022, VIEW SERVER PERFORMANCE STATE).";

    private static readonly string[] SystemSchemas =
    [
        "sys", "guest", "INFORMATION_SCHEMA",
        "db_owner", "db_accessadmin", "db_securityadmin", "db_ddladmin", "db_backupoperator",
        "db_datareader", "db_datawriter", "db_denydatareader", "db_denydatawriter"
    ];

    /// <summary>Permissions that, granted on the database, let the login change data or schema.</summary>
    private static readonly string[] WritePermissions =
    [
        "INSERT", "UPDATE", "DELETE", "ALTER", "CONTROL", "TAKE OWNERSHIP", "ALTER ANY SCHEMA",
        "CREATE TABLE", "CREATE VIEW", "CREATE PROCEDURE", "CREATE FUNCTION", "CREATE SCHEMA"
    ];

    /// <summary>
    /// The handful of facts that decide what the other tools can see, read in one round trip.
    /// </summary>
    internal sealed record AccessProbe(
        bool ViewDefinition,
        bool SelectOnDatabase,
        bool ReadSqlDependencies,
        bool ReadServerState,
        int ModulesVisible,
        int ModulesDefinitionHidden,
        int ProductMajorVersion)
    {
        /// <summary>Every procedure, function and view is visible, with its definition.</summary>
        public bool ModulesComplete => ViewDefinition;

        /// <summary>Every table and view is visible.</summary>
        public bool TablesComplete => ViewDefinition || SelectOnDatabase;

        /// <summary>Module references are reported for every module.</summary>
        public bool SqlDependenciesComplete => ReadSqlDependencies && ViewDefinition;
    }

    internal AccessProbe ProbeAccess(SqlConnection conn)
    {
        // HAS_PERMS_BY_NAME answers NULL for a permission the server does not know, which is how
        // VIEW SERVER PERFORMANCE STATE reads before SQL Server 2022; then VIEW SERVER STATE decides.
        using var cmd = CreateCommand(conn, """
            SELECT
                CAST(HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'VIEW DEFINITION') AS bit),
                CAST(HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'SELECT') AS bit),
                CAST(ISNULL(HAS_PERMS_BY_NAME('sys.sql_expression_dependencies', 'OBJECT', 'SELECT'), 0) AS bit),
                CAST(ISNULL(COALESCE(HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER PERFORMANCE STATE'),
                                     HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER STATE')), 0) AS bit),
                (SELECT COUNT(*) FROM sys.sql_modules),
                (SELECT COUNT(*) FROM sys.sql_modules WHERE definition IS NULL),
                CAST(SERVERPROPERTY('ProductMajorVersion') AS int);
            """);
        using var reader = cmd.ExecuteReader();
        reader.Read();
        return new AccessProbe(
            reader.GetBoolean(0),
            reader.GetBoolean(1),
            reader.GetBoolean(2),
            reader.GetBoolean(3),
            reader.GetInt32(4),
            reader.GetInt32(5),
            reader.IsDBNull(6) ? 0 : reader.GetInt32(6));
    }

    /// <summary>
    /// The block a response carries to say whether it shows everything. <c>complete</c> is always
    /// present; the notes and the hint appear only when something is missing.
    /// </summary>
    private static Dictionary<string, object?> Visibility(IEnumerable<string?> notes)
    {
        var missing = notes.Where(n => n is not null).ToList();
        var block = new Dictionary<string, object?> { ["complete"] = missing.Count == 0 };
        if (missing.Count == 0) return block;

        block["notes"] = missing;
        block["hint"] = CheckPermissionsHint;
        return block;
    }

    private static string? DefinitionsHiddenNote(AccessProbe probe) =>
        probe.ModulesDefinitionHidden == 0
            ? null
            : $"{probe.ModulesDefinitionHidden} of the {probe.ModulesVisible} visible views, procedures "
              + "and functions have their definition hidden from this login.";

    private static string? SqlDependenciesNote(AccessProbe probe) =>
        !probe.ReadSqlDependencies ? NoSqlDependenciesNote
        : !probe.ViewDefinition ? FilteredSqlDependenciesNote
        : null;

    /// <summary>
    /// Reports what the connected login is, what it holds, what each tool can therefore see, the
    /// GRANT statements that would complete it, and whether the login could write.
    /// </summary>
    public string CheckPermissions()
    {
        using var conn = OpenConnection();
        var probe = ProbeAccess(conn);

        string login, originalLogin, user, database, productVersion, edition;
        using (var cmd = CreateCommand(conn, """
            SELECT SUSER_SNAME(), ORIGINAL_LOGIN(), USER_NAME(), DB_NAME(),
                   CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)),
                   CAST(SERVERPROPERTY('Edition') AS nvarchar(128));
            """))
        using (var reader = cmd.ExecuteReader())
        {
            reader.Read();
            login = reader.GetString(0);
            originalLogin = reader.GetString(1);
            user = reader.GetString(2);
            database = reader.GetString(3);
            productVersion = reader.IsDBNull(4) ? "" : reader.GetString(4);
            edition = reader.IsDBNull(5) ? "" : reader.GetString(5);
        }

        // sys.user_token and sys.login_token list the token's roles and groups, nested ones included,
        // without needing VIEW DEFINITION on the principals.
        var databaseRoles = ReadStrings(conn,
            "SELECT name FROM sys.user_token WHERE type = 'ROLE' ORDER BY name;");
        var serverRoles = ReadStrings(conn,
            "SELECT name FROM sys.login_token WHERE type = 'SERVER ROLE' ORDER BY name;");
        var windowsGroups = ReadStrings(conn,
            "SELECT name FROM sys.login_token WHERE type = 'WINDOWS GROUP' ORDER BY name;");
        var databasePermissions = ReadStrings(conn,
            "SELECT permission_name FROM fn_my_permissions(NULL, 'DATABASE') ORDER BY permission_name;");
        var serverPermissions = ReadStrings(conn,
            "SELECT permission_name FROM fn_my_permissions(NULL, 'SERVER') ORDER BY permission_name;");

        var needs = new GrantNeeds(
            ViewDefinition: !probe.ViewDefinition,
            SelectOnDatabase: !probe.SelectOnDatabase,
            SqlDependencies: !probe.ReadSqlDependencies,
            ServerState: !probe.ReadServerState);

        var result = new Dictionary<string, object?>
        {
            ["identity"] = new
            {
                login,
                original_login = originalLogin,
                database_user = user,
                database,
                server_version = new
                {
                    major = probe.ProductMajorVersion,
                    name = ProductName(probe.ProductMajorVersion),
                    product_version = productVersion,
                    edition
                }
            },
            ["roles"] = new { database = databaseRoles, server = serverRoles, windows_groups = windowsGroups },
            ["database_permissions"] = databasePermissions,
            ["server_permissions"] = serverPermissions,
            ["modules"] = new
            {
                visible = probe.ModulesVisible,
                definition_hidden = probe.ModulesDefinitionHidden
            }
        };

        // Without VIEW DEFINITION on the database it may still be granted schema by schema, which
        // the database-level check cannot see; this says which schemas are fully readable.
        if (!probe.ViewDefinition)
            result["schema_view_definition"] = ReadSchemaViewDefinition(conn);

        result["tools"] = DescribeTools(probe, user, login);
        result["grant_script"] = BuildGrantScript(database, user, login, probe.ProductMajorVersion, needs);
        if (windowsGroups.Count > 0)
            result["grant_note"] =
                "This is a Windows login. If it reaches the database through one of its groups rather than "
                + "a user of its own, the GRANT goes to that group's user or login instead.";
        result["write_access"] = DescribeWriteAccess(conn, databasePermissions);

        return JsonSerializer.Serialize(result);
    }

    private object ReadSchemaViewDefinition(SqlConnection conn)
    {
        var excluded = string.Join(", ", SystemSchemas.Select(s => $"'{s}'"));
        using var cmd = CreateCommand(conn, $"""
            SELECT s.name,
                   CAST(HAS_PERMS_BY_NAME(QUOTENAME(s.name), 'SCHEMA', 'VIEW DEFINITION') AS bit)
            FROM sys.schemas s
            WHERE s.name NOT IN ({excluded})
            ORDER BY s.name;
            """);
        var granted = new List<string>();
        var missing = new List<string>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
            (reader.GetBoolean(1) ? granted : missing).Add(reader.GetString(0));

        return new { granted, missing };
    }

    private static List<object> DescribeTools(AccessProbe probe, string user, string login)
    {
        var u = QuoteName(user);
        var tools = new List<object>
        {
            new
            {
                area = "tables_and_views",
                tools = new[] { "list_tables", "list_views", "get_table_schema", "find_columns",
                                "get_schema_summary", "compare_schemas", "query_table", "get_data_profile" },
                status = probe.SelectOnDatabase ? "complete" : "partial",
                effect = probe.SelectOnDatabase ? null
                    : probe.ViewDefinition
                        ? "Every table and view is listed, but only those the login holds SELECT on can be queried or profiled."
                        : "Only tables and views the login holds a permission on are listed, and only those it holds SELECT on can be queried.",
                missing = probe.SelectOnDatabase ? null : "SELECT on the database (db_datareader)",
                grant = probe.SelectOnDatabase ? null : $"ALTER ROLE db_datareader ADD MEMBER {u};"
            },
            new
            {
                area = "module_definitions",
                tools = new[] { "list_procedures", "list_functions", "get_procedure_definition",
                                "get_function_definition", "get_view_definition", "get_database_overview" },
                status = probe.ViewDefinition ? "complete" : "partial",
                effect = probe.ViewDefinition ? null
                    : $"Procedures and functions the login holds no permission on are left out without an error. "
                      + $"Of the {probe.ModulesVisible} views, procedures and functions it can see, "
                      + $"{probe.ModulesDefinitionHidden} have their definition hidden.",
                missing = probe.ViewDefinition ? null : "VIEW DEFINITION on the database",
                grant = probe.ViewDefinition ? null : $"GRANT VIEW DEFINITION TO {u};"
            },
            new
            {
                area = "sql_dependencies",
                tools = new[] { "get_dependency_graph", "generate_dependency_dot", "get_table_usage",
                                "list_procedures (referenced_object_count)", "list_functions (referenced_object_count)" },
                status = !probe.ReadSqlDependencies ? "unavailable" : probe.ViewDefinition ? "complete" : "partial",
                effect = SqlDependenciesNote(probe),
                missing = (probe.ReadSqlDependencies, probe.ViewDefinition) switch
                {
                    (true, true) => null,
                    (true, false) => "VIEW DEFINITION on the database",
                    (false, true) => "SELECT on sys.sql_expression_dependencies",
                    (false, false) => "SELECT on sys.sql_expression_dependencies and VIEW DEFINITION on the database"
                },
                grant = (probe.ReadSqlDependencies, probe.ViewDefinition) switch
                {
                    (true, true) => null,
                    (true, false) => $"GRANT VIEW DEFINITION TO {u};",
                    (false, true) => $"GRANT SELECT ON sys.sql_expression_dependencies TO {u};",
                    (false, false) => $"GRANT VIEW DEFINITION TO {u}; GRANT SELECT ON sys.sql_expression_dependencies TO {u};"
                }
            },
            new
            {
                area = "index_usage",
                tools = new[] { "get_index_health (unused_indexes, missing_index_suggestions)" },
                status = probe.ReadServerState ? "complete" : "partial",
                effect = probe.ReadServerState ? null
                    : "get_index_health returns duplicate_indexes only; the sections that read DMVs come back unavailable.",
                missing = probe.ReadServerState ? null : ServerStatePermission(probe.ProductMajorVersion) + " on the server",
                grant = probe.ReadServerState ? null
                    : $"USE master; GRANT {ServerStatePermission(probe.ProductMajorVersion)} TO {QuoteName(login)};"
            }
        };
        return tools;
    }

    /// <summary>Which permissions <see cref="BuildGrantScript"/> should grant.</summary>
    internal sealed record GrantNeeds(bool ViewDefinition, bool SelectOnDatabase, bool SqlDependencies, bool ServerState)
    {
        public bool Any => ViewDefinition || SelectOnDatabase || SqlDependencies || ServerState;
    }

    /// <summary>
    /// The statements that give the login what it is missing, in the syntax of the server's version.
    /// Database permissions go to the database user; the server one goes to the login, from master.
    /// Null when nothing is missing.
    /// </summary>
    /// <remarks>
    /// Each area in the report names its own minimal grant; this script is the union of them, so it
    /// drops what another line already covers.
    /// </remarks>
    internal static string? BuildGrantScript(string database, string user, string login, int productMajorVersion, GrantNeeds needs)
    {
        if (!needs.Any) return null;

        var lines = new List<string>();
        if (needs.ViewDefinition || needs.SelectOnDatabase || needs.SqlDependencies)
        {
            lines.Add($"USE {QuoteName(database)};");
            if (needs.SelectOnDatabase) lines.Add($"ALTER ROLE db_datareader ADD MEMBER {QuoteName(user)};");
            if (needs.ViewDefinition) lines.Add($"GRANT VIEW DEFINITION TO {QuoteName(user)};");
            // db_datareader already carries SELECT on the view; granting it again would only be noise.
            if (needs.SqlDependencies && !needs.SelectOnDatabase)
                lines.Add($"GRANT SELECT ON sys.sql_expression_dependencies TO {QuoteName(user)};");
        }

        if (needs.ServerState)
        {
            lines.Add("USE master;");
            lines.Add($"GRANT {ServerStatePermission(productMajorVersion)} TO {QuoteName(login)};");
        }

        return string.Join("\n", lines);
    }

    /// <summary>
    /// SQL Server 2022 (major version 16) split VIEW SERVER STATE into finer permissions, and the
    /// index DMVs need only the performance one. Earlier versions know only VIEW SERVER STATE.
    /// </summary>
    internal static string ServerStatePermission(int productMajorVersion) =>
        productMajorVersion >= 16 ? "VIEW SERVER PERFORMANCE STATE" : "VIEW SERVER STATE";

    internal static string ProductName(int productMajorVersion) => productMajorVersion switch
    {
        14 => "SQL Server 2017",
        15 => "SQL Server 2019",
        16 => "SQL Server 2022",
        17 => "SQL Server 2025",
        _ => $"SQL Server (major version {productMajorVersion})"
    };

    private static string QuoteName(string name) => $"[{name.Replace("]", "]]")}]";

    /// <summary>
    /// The other side of the README's advice to use a read-only account: says when this one is not.
    /// </summary>
    /// <remarks>
    /// Database-level permissions and role memberships are not enough on their own: a GRANT on a
    /// schema or a single object never shows at that level, and an account named "read-only" with
    /// INSERT on a dozen tables is exactly what this exists to catch. <c>sys.database_permissions</c>,
    /// filtered to the principals in the login's token, covers those. EXECUTE is listed but does not
    /// decide the verdict: this server never executes anything, and nearly every database grants it
    /// to public on the diagram procedures.
    /// </remarks>
    private object DescribeWriteAccess(SqlConnection conn, List<string> databasePermissions)
    {
        var writerRoles = new List<string>();
        using (var cmd = CreateCommand(conn, """
            SELECT IS_SRVROLEMEMBER('sysadmin'), IS_MEMBER('db_owner'), IS_MEMBER('db_datawriter'), IS_MEMBER('db_ddladmin');
            """))
        using (var reader = cmd.ExecuteReader())
        {
            reader.Read();
            string[] names = ["sysadmin", "db_owner", "db_datawriter", "db_ddladmin"];
            for (var i = 0; i < names.Length; i++)
                if (!reader.IsDBNull(i) && reader.GetInt32(i) == 1)
                    writerRoles.Add(names[i]);
        }

        var writeOnDatabase = databasePermissions
            .Where(p => WritePermissions.Contains(p, StringComparer.OrdinalIgnoreCase))
            .ToList();

        var grants = new List<Dictionary<string, object?>>();
        using (var cmd = CreateCommand(conn, """
            SELECT dp.class_desc,
                   dp.permission_name,
                   USER_NAME(dp.grantee_principal_id) AS grantee,
                   COUNT(*) AS grant_count
            FROM sys.database_permissions dp
            WHERE dp.grantee_principal_id IN (SELECT principal_id FROM sys.user_token)
              AND dp.state IN ('G', 'W')
              AND dp.class IN (1, 3)
              AND dp.permission_name IN ('INSERT', 'UPDATE', 'DELETE', 'ALTER', 'CONTROL', 'TAKE OWNERSHIP', 'EXECUTE')
            GROUP BY dp.class_desc, dp.permission_name, dp.grantee_principal_id
            ORDER BY dp.permission_name, dp.class_desc, grantee;
            """))
        using (var reader = cmd.ExecuteReader())
        {
            while (reader.Read())
            {
                grants.Add(new Dictionary<string, object?>
                {
                    ["scope"] = reader.GetString(0) == "SCHEMA" ? "schema" : "object",
                    ["permission"] = reader.GetString(1),
                    ["grantee"] = reader.IsDBNull(2) ? null : reader.GetString(2),
                    ["count"] = reader.GetInt32(3)
                });
            }
        }

        var writeGrants = grants.Where(g => (string)g["permission"]! != "EXECUTE").ToList();
        var readOnly = writerRoles.Count == 0 && writeOnDatabase.Count == 0 && writeGrants.Count == 0;

        return new
        {
            read_only = readOnly,
            roles = writerRoles,
            database_permissions = writeOnDatabase,
            schema_and_object_grants = grants,
            note = readOnly
                ? "No role, database permission or schema/object GRANT lets this login change data or schema. "
                  + "EXECUTE grants, if listed, are not counted: this server never executes procedures."
                : "This login can change data or schema. The server itself only reads, but the README recommends "
                  + "a read-only account so that nothing else holding these credentials can write either."
        };
    }

    private List<string> ReadStrings(SqlConnection conn, string sql)
    {
        using var cmd = CreateCommand(conn, sql);
        using var reader = cmd.ExecuteReader();
        var values = new List<string>();
        while (reader.Read())
            if (!reader.IsDBNull(0)) values.Add(reader.GetString(0));
        return values;
    }
}
