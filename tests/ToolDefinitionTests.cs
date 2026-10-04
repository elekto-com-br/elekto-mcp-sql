using DescriptionAttribute = System.ComponentModel.DescriptionAttribute;
using System.Reflection;
using Elekto.Mcp.Sql.Tools;
using ModelContextProtocol.Server;

namespace Elekto.Mcp.Sql.Tests;

/// <summary>
/// What each tool publishes in tools/list is all an agent has to choose it by. These tests stop a
/// new or changed tool from shipping again without a title, without annotations, or without saying
/// when to use another tool.
/// </summary>
[TestFixture]
public class ToolDefinitionTests
{
    private static IEnumerable<MethodInfo> Tools() =>
        typeof(SqlTools).GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null);

    private static IEnumerable<TestCaseData> ToolCases() =>
        Tools().Select(m => new TestCaseData(m).SetName($"Tool_{m.Name}"));

    [Test]
    public void ThereAreTwentyOneTools() => Assert.That(Tools().Count(), Is.EqualTo(21));

    [TestCaseSource(nameof(ToolCases))]
    public void DeclaresTitleAndReadOnlyAnnotations(MethodInfo method)
    {
        var tool = method.GetCustomAttribute<McpServerToolAttribute>()!;

        Assert.Multiple(() =>
        {
            Assert.That(tool.Title, Is.Not.Null.And.Not.Empty, "Title");
            Assert.That(tool.Title, Is.Not.EqualTo(method.Name), "Title must say more than the name");
            Assert.That(tool.ReadOnly, Is.True, "ReadOnly");
            Assert.That(tool.Destructive, Is.False, "Destructive");
            Assert.That(tool.Idempotent, Is.True, "Idempotent");
            Assert.That(tool.OpenWorld, Is.False, "OpenWorld");
        });
    }

    [TestCaseSource(nameof(ToolCases))]
    public void DescriptionSaysWhenToUseAnotherTool(MethodInfo method)
    {
        var description = method.GetCustomAttribute<DescriptionAttribute>()?.Description ?? "";
        var siblings = Tools().Select(m => m.Name).Where(n => n != method.Name);

        Assert.Multiple(() =>
        {
            Assert.That(description.Length, Is.GreaterThanOrEqualTo(150), "too short to be useful");
            Assert.That(siblings.Any(description.Contains), Is.True,
                "the description should name the tool to use instead, or next");
        });
    }

    [Test]
    public void EveryParameterIsDescribed()
    {
        var undocumented = Tools()
            .SelectMany(m => m.GetParameters().Select(p => (Tool: m.Name, Parameter: p)))
            .Where(x => string.IsNullOrWhiteSpace(x.Parameter.GetCustomAttribute<DescriptionAttribute>()?.Description))
            .Select(x => $"{x.Tool}.{x.Parameter.Name}");

        Assert.That(undocumented, Is.Empty);
    }

    [Test]
    public void InstructionsNameEveryEntryPoint()
    {
        Assert.That(ServerInstructions.Usage, Does.Contain("list_databases").And.Contain("check_permissions"));
        Assert.That(ServerInstructions.For("Missing."), Does.StartWith("Missing."));
    }
}
