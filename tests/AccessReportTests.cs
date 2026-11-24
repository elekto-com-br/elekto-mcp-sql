// Copyright (c) 2026 Elekto Produtos Financeiros. Licensed under the GNU General Public License v3.0 (GPL-3.0).
// This software is provided "as is", without warranty of any kind. Use at your own risk.
// See the LICENSE file for the full license text.

using Elekto.Mcp.Sql.Data;

namespace Elekto.Mcp.Sql.Tests;

/// <summary>
/// The parts of the permission report and of the error hints that need no server: the GRANT
/// statements per version, and which hint each SQL Server error number earns.
/// </summary>
[TestFixture]
public class AccessReportTests
{
    private static readonly SchemaReader.GrantNeeds Everything = new(true, true, true, true);

    [TestCase(14, "VIEW SERVER STATE")]
    [TestCase(15, "VIEW SERVER STATE")]
    [TestCase(16, "VIEW SERVER PERFORMANCE STATE")]
    [TestCase(17, "VIEW SERVER PERFORMANCE STATE")]
    public void ServerStatePermission_FollowsTheVersion(int major, string expected) =>
        Assert.That(SchemaReader.ServerStatePermission(major), Is.EqualTo(expected));

    [Test]
    public void GrantScript_On2019_GrantsViewServerState()
    {
        var script = SchemaReader.BuildGrantScript("Risk", "reader", "reader_login", 15, Everything);

        Assert.That(script, Is.EqualTo("""
            USE [Risk];
            ALTER ROLE db_datareader ADD MEMBER [reader];
            GRANT VIEW DEFINITION TO [reader];
            USE master;
            GRANT VIEW SERVER STATE TO [reader_login];
            """.ReplaceLineEndings("\n")));
    }

    [Test]
    public void GrantScript_On2022_GrantsOnlyThePerformanceState()
    {
        var script = SchemaReader.BuildGrantScript("Risk", "reader", "reader_login", 16, Everything);

        Assert.That(script, Does.EndWith("GRANT VIEW SERVER PERFORMANCE STATE TO [reader_login];"));
    }

    [Test]
    public void GrantScript_DatabaseOnly_DoesNotTouchMaster()
    {
        var script = SchemaReader.BuildGrantScript("Risk", "reader", "reader_login", 15,
            new SchemaReader.GrantNeeds(ViewDefinition: true, SelectOnDatabase: false, SqlDependencies: false, ServerState: false));

        Assert.That(script, Is.EqualTo("USE [Risk];\nGRANT VIEW DEFINITION TO [reader];"));
    }

    [Test]
    public void GrantScript_ServerOnly_GoesToTheLoginFromMaster()
    {
        var script = SchemaReader.BuildGrantScript("Risk", "reader", "reader_login", 15,
            new SchemaReader.GrantNeeds(ViewDefinition: false, SelectOnDatabase: false, SqlDependencies: false, ServerState: true));

        Assert.That(script, Is.EqualTo("USE master;\nGRANT VIEW SERVER STATE TO [reader_login];"));
    }

    [Test]
    public void GrantScript_OutsideDbDatareader_LetsTheRoleCoverTheDependencyView()
    {
        var script = SchemaReader.BuildGrantScript("Risk", "reader", "reader_login", 15,
            new SchemaReader.GrantNeeds(ViewDefinition: false, SelectOnDatabase: true, SqlDependencies: true, ServerState: false));

        Assert.That(script, Is.EqualTo("USE [Risk];\nALTER ROLE db_datareader ADD MEMBER [reader];"),
            "db_datareader already reaches sys.sql_expression_dependencies.");
    }

    [Test]
    public void GrantScript_WithSelectOnTheDatabase_GrantsTheDependencyViewItIsStillMissing()
    {
        // SELECT on the database yet not on the view, as after a DENY.
        var script = SchemaReader.BuildGrantScript("Risk", "reader", "reader_login", 15,
            new SchemaReader.GrantNeeds(ViewDefinition: false, SelectOnDatabase: false, SqlDependencies: true, ServerState: false));

        Assert.That(script, Is.EqualTo("USE [Risk];\nGRANT SELECT ON sys.sql_expression_dependencies TO [reader];"));
    }

    [Test]
    public void GrantScript_NothingMissing_IsNull() =>
        Assert.That(SchemaReader.BuildGrantScript("Risk", "reader", "reader_login", 15,
            new SchemaReader.GrantNeeds(false, false, false, false)), Is.Null);

    [Test]
    public void GrantScript_QuotesNamesThatNeedIt()
    {
        var script = SchemaReader.BuildGrantScript("My]Db", @"DOMAIN\joe", @"DOMAIN\joe", 15,
            new SchemaReader.GrantNeeds(ViewDefinition: true, SelectOnDatabase: false, SqlDependencies: false, ServerState: false));

        Assert.That(script, Is.EqualTo("USE [My]]Db];\nGRANT VIEW DEFINITION TO [DOMAIN\\joe];"));
    }

    [TestCase(229)]
    [TestCase(262)]
    [TestCase(297)]
    [TestCase(300)]
    [TestCase(916)]
    [TestCase(15562)]
    public void ErrorHint_PermissionErrors_PointToCheckPermissions(int number) =>
        Assert.That(SqlErrorHint.For([number]), Is.EqualTo(SqlErrorHint.Permission));

    [TestCase(207)]
    [TestCase(208)]
    public void ErrorHint_MissingNames_SayTheNameMayBeHidden(int number) =>
        Assert.That(SqlErrorHint.For([number]), Is.EqualTo(SqlErrorHint.Name));

    [TestCase(102)]
    [TestCase(156)]
    public void ErrorHint_SyntaxErrors_PointToTheClauses(int number) =>
        Assert.That(SqlErrorHint.For([number]), Is.EqualTo(SqlErrorHint.Syntax));

    [Test]
    public void ErrorHint_TakesTheFirstErrorOfAKnownKind() =>
        Assert.That(SqlErrorHint.For([50000, 229, 208]), Is.EqualTo(SqlErrorHint.Permission));

    [Test]
    public void ErrorHint_UnknownNumber_FallsBackToTheGeneralHint() =>
        Assert.That(SqlErrorHint.For([50000]), Is.EqualTo(SqlErrorHint.Unknown));

    [TestCase(4060)]
    [TestCase(4064)]
    [TestCase(15113)]
    [TestCase(18452)]
    [TestCase(18456)]
    [TestCase(18470)]
    [TestCase(18486)]
    [TestCase(18487)]
    [TestCase(18488)]
    public void ErrorHint_RefusedLogins_PointToTheConfiguration(int number) =>
        Assert.That(SqlErrorHint.For([number]), Is.EqualTo(SqlErrorHint.Login));

    [TestCase(2)]
    [TestCase(53)]
    [TestCase(64)]
    [TestCase(121)]
    [TestCase(232)]
    [TestCase(233)]
    [TestCase(258)]
    [TestCase(1225)]
    [TestCase(1231)]
    [TestCase(1232)]
    [TestCase(1236)]
    [TestCase(10050)]
    [TestCase(10051)]
    [TestCase(10053)]
    [TestCase(10054)]
    [TestCase(10060)]
    [TestCase(10061)]
    [TestCase(10064)]
    [TestCase(10065)]
    [TestCase(11001)]
    public void ErrorHint_NetworkErrors_SayTheCallIsNotAtFault(int number) =>
        Assert.That(SqlErrorHint.For([number]), Is.EqualTo(SqlErrorHint.Connection));

    [Test]
    public void ErrorHint_TimeoutWhileQuerying_IsAQueryTimeout() =>
        Assert.That(SqlErrorHint.For([-2]), Is.EqualTo(SqlErrorHint.Timeout));

    [Test]
    public void ErrorHint_TimeoutWhileConnecting_IsAFailedConnection() =>
        Assert.That(SqlErrorHint.For([-2], connecting: true), Is.EqualTo(SqlErrorHint.Connection));

    // While connecting no query has run, so whatever the number, the connection is what failed.
    // SqlClient numbers "Error Locating Server/Instance Specified" -1, for one, which says nothing
    // on its own.
    [TestCase(-1)]
    [TestCase(0)]
    [TestCase(208)]
    [TestCase(50000)]
    public void ErrorHint_AnyErrorWhileConnecting_IsAFailedConnection(int number) =>
        Assert.That(SqlErrorHint.For([number], connecting: true), Is.EqualTo(SqlErrorHint.Connection));

    [Test]
    public void ErrorHint_RefusedLoginWhileConnecting_StillPointsToTheConfiguration() =>
        Assert.That(SqlErrorHint.For([4060, 18456], connecting: true), Is.EqualTo(SqlErrorHint.Login));

    [Test]
    public void ErrorHint_ConnectionAndLoginHints_DoNotSendTheCallerToTheObjects()
    {
        foreach (var hint in new[] { SqlErrorHint.Connection, SqlErrorHint.Login })
            Assert.That(hint, Does.Not.Contain("list_tables").And.Not.Contain("get_table_schema"));
    }
}
