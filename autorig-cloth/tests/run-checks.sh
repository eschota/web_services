#!/usr/bin/env bash
# Runs every automated check of AutoRig Cloth and exits non-zero if any fails:
#   1. xUnit tests of package/Runtime/Core, compiled without any Unity reference;
#   2. package/Runtime and package/Samples~ compiled against the Unity 2021.3 engine reference
#      assemblies (NuGet UnityEngine.Modules), warnings as errors, XML docs required;
#   3. package/Editor compiled against UnityEditor/UnityEngine reference assemblies
#      (NuGet Unity3D.SDK) together with the runtime;
#   4. every asset in the package has a .meta file (git-installed packages are immutable, and
#      Unity ignores files without one), no orphan .meta files, no duplicate GUIDs.
# Needs the .NET 8 SDK and access to nuget.org. Set NUGET_PACKAGES to keep the package cache
# somewhere other than ~/.nuget/packages.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKAGE="$(cd "$HERE/../package" && pwd)"
export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1 DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1

step() { printf '\n==> %s\n' "$1"; }

step "Core unit tests (package/Runtime/Core, no UnityEngine)"
# Detailed verbosity shows test output (the performance timing); individual "Passed" lines are dropped.
dotnet test "$HERE/AutoRig.Cloth.Core.Tests/AutoRig.Cloth.Core.Tests.csproj" -c Release --nologo \
    --logger "console;verbosity=detailed" | { grep -Ev '^\s+Passed [A-Za-z]|^\s*$' || true; }

step "Compile check: Runtime and Samples against the UnityEngine 2021.3 reference assemblies"
dotnet build "$HERE/compile-check/Samples/AutoRig.Cloth.Samples.csproj" -c Release --nologo -v quiet

step "Compile check: Editor against the UnityEditor reference assemblies"
dotnet build "$HERE/compile-check/Editor/AutoRig.Cloth.Editor.csproj" -c Release --nologo -v quiet

step "Unity .meta files"
problems=0
# Folders ending in '~' and hidden files are ignored by Unity and need no .meta.
while IFS= read -r -d '' asset; do
    if [[ ! -f "$asset.meta" ]]; then
        echo "missing .meta: ${asset#"$PACKAGE"/}"
        problems=1
    fi
done < <(find "$PACKAGE" -mindepth 1 \( -name '*~' -o -name '.*' \) -prune -o ! -name '*.meta' -print0)
while IFS= read -r -d '' meta; do
    if [[ ! -e "${meta%.meta}" ]]; then
        echo "orphan .meta: ${meta#"$PACKAGE"/}"
        problems=1
    fi
done < <(find "$PACKAGE" -mindepth 1 \( -name '*~' -o -name '.*' \) -prune -o -name '*.meta' -print0)
duplicates="$(find "$PACKAGE" -mindepth 1 \( -name '*~' -o -name '.*' \) -prune -o -name '*.meta' -print0 \
    | xargs -0 grep -h '^guid:' | sort | uniq -d)"
if [[ -n "$duplicates" ]]; then
    echo "duplicate GUIDs:"
    echo "$duplicates"
    problems=1
fi
if [[ "$problems" -ne 0 ]]; then
    echo "Fix the .meta files (opening the package from a Unity project's Packages folder generates them)."
    exit 1
fi
echo "All package assets have .meta files."

printf '\nAll checks passed.\n'
