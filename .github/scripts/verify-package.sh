#!/usr/bin/env bash
# Checks the package that `dotnet pack` wrote to the folder given, the same way in CI and in the
# publish workflow, before anything is shipped: exactly one .nupkg, holding the tool, marked as an
# MCP server, with a .mcp/server.json that declares this package at the csproj version and a README
# that names the server, without which the MCP Registry refuses the package.
set -euo pipefail

DIST=${1:?usage: verify-package.sh <folder holding the .nupkg>}
CSPROJ=src/Elekto.Mcp.Sql.csproj

VERSION=$(sed -n 's:.*<Version>\(.*\)</Version>.*:\1:p' "$CSPROJ" | head -1)

mapfile -t PACKAGES < <(find "$DIST" -maxdepth 1 -name '*.nupkg')
if [ "${#PACKAGES[@]}" -ne 1 ]; then
  echo "::error::Expected one .nupkg in ${DIST}, found ${#PACKAGES[@]}."
  exit 1
fi
NUPKG=${PACKAGES[0]}

# Each part is extracted to a file before it is searched: grep -q on a pipe exits at the first
# match, and with pipefail the writer it leaves behind would fail the check
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

unzip -Z1 "$NUPKG" > "$WORK/files.txt"
for f in tools/net10.0/any/Elekto.Mcp.Sql.dll tools/net10.0/any/DotnetToolSettings.xml; do
  grep -qxF "$f" "$WORK/files.txt" \
    || { echo "::error::The package lacks ${f}, so it would not install as a .NET tool."; exit 1; }
done

unzip -p "$NUPKG" Elekto.Mcp.Sql.nuspec > "$WORK/package.nuspec" \
  && grep -q '<packageType name="McpServer" />' "$WORK/package.nuspec" \
  || { echo "::error::The package is not marked as an MCP server (PackageType McpServer)."; exit 1; }

unzip -p "$NUPKG" .mcp/server.json > "$WORK/server.json" \
  || { echo "::error::The package carries no .mcp/server.json."; exit 1; }

jq -e --arg v "$VERSION" \
  '.version == $v and .packages[0].version == $v and .packages[0].identifier == "Elekto.Mcp.Sql"' \
  "$WORK/server.json" > /dev/null \
  || { echo "::error::.mcp/server.json in the package does not declare Elekto.Mcp.Sql ${VERSION}."; cat "$WORK/server.json"; exit 1; }

# The MCP Registry accepts the package only if its README names the server
NAME=$(jq -r .name "$WORK/server.json")
unzip -p "$NUPKG" README.md > "$WORK/README.md" \
  && grep -qF "mcp-name: ${NAME}" "$WORK/README.md" \
  || { echo "::error::The packed README lacks 'mcp-name: ${NAME}', without which the MCP Registry refuses the package."; exit 1; }

echo "Package ${VERSION} holds the tool, is an MCP server, its manifest agrees, and its README names ${NAME}."
