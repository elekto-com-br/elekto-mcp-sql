# Elekto.Mcp.Sql

Read-only MCP server for SQL Server, written in C# (.NET 10), distributed as a .NET tool on NuGet
(`Elekto.Mcp.Sql`) and listed in the MCP Registry as `io.github.elekto-com-br/elekto-mcp-sql`.

## Language

Everything in this repository is in English: code, comments, test names, commit messages,
documentation and the text the tools return. This holds even when the conversation with the
maintainer is in Portuguese. The only Portuguese is deliberate test data (accented extended
property values in `tests/Infrastructure/TestDatabase.cs`), which exercises Unicode text.

Do not use en or em dashes in prose written for this repository; use commas, colons, parentheses
or separate sentences.

## Line endings

LF everywhere (`.gitattributes`: `* text=auto eol=lf`). Never introduce CRLF.

## Layout

- `src/Program.cs`: host setup, stdio transport, server instructions.
- `src/Configuration/`: where connections come from (`ConnectionConfig`), the registry that lets
  the server start without any (`ConnectionRegistry`), and the guidance returned when none is
  configured (`SetupGuide`).
- `src/Tools/SqlTools.cs`: the 21 MCP tools. `ToolResponse` turns failures into `ok: false`
  content; `ServerInstructions` is what clients show the model on connecting.
- `src/Data/`: `SchemaReader` holds all SQL; every query is read-only.
- `src/.mcp/server.json`: MCP manifest packed into the NuGet package; `$version$` is replaced at
  pack time, so it needs no edit for a release.

## Tools are the product

What a tool publishes in `tools/list` is all an agent has to choose it by, and Glama scores it
(TDQS). Every tool must keep its title, the read-only annotations, a described parameter list, and
a description that says what it does, when to use it and which sibling to use instead.
`tests/ToolDefinitionTests.cs` enforces this. Never claim in a description what the code does not
do; check `SchemaReader` first.

Optional parameters are non-nullable with a sentinel default (`""` or `0`); see the remarks on
`SqlTools` for why.

## Tests

```bash
dotnet build -c Release
dotnet test --no-build -c Release
```

Integration tests need a SQL Server: `ELEKTO_MCP_SQL_CONN_TEST`, LocalDB on Windows, or a
Testcontainers container through Docker. Unit tests alone:
`dotnet test --filter "FullyQualifiedName!~SchemaReaderTests"`.

## Releases

1. Add a `## What changed in X.Y.Z` section to `README.md`; it becomes the GitHub Release notes.
2. Set `<Version>` in `src/Elekto.Mcp.Sql.csproj`; it must equal the tag.
3. Commit as `chore(release): X.Y.Z`, push, and wait for CI on `main` to pass.
4. Push the annotated tag: `git tag -a vX.Y.Z -m "X.Y.Z" && git push origin vX.Y.Z`.

The tag runs `.github/workflows/publish.yml`: tests, NuGet push, then the GitHub Release and the
MCP Registry publication (which waits for nuget.org to serve the package). Glama rebuilds on each
GitHub Release.

The README must keep `<!-- mcp-name: io.github.elekto-com-br/elekto-mcp-sql -->`: the MCP Registry
reads it to accept the package, and CI fails without it.
