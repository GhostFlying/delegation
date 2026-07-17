#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
plugin_root="$repo_root/plugins/delegation"
tmp=$(mktemp -d)
cleanup() {
  rm -rf "$tmp"
}
trap cleanup EXIT HUP INT TERM
go_bin=${GO:-go}
"$go_bin" -C "$repo_root" build -trimpath -o "$tmp/delegation" ./cmd/delegation

if DELEGATION_HOME="$tmp/missing" "$plugin_root/scripts/delegation-mcp" mcp root >"$tmp/out" 2>"$tmp/err"; then
  printf '%s\n' 'expected missing runtime launcher to fail' >&2
  exit 1
fi
grep -F 'runtime 0.1.0-alpha.0 is not installed' "$tmp/err" >/dev/null

DELEGATION_BINARY="$tmp/delegation" "$plugin_root/scripts/delegation-mcp" version --json >"$tmp/version"
grep -F '"version":"0.1.0-alpha.0"' "$tmp/version" >/dev/null

cp -R "$plugin_root" "$tmp/plugin"
mkdir -p "$tmp/payload" "$tmp/fake-bin"
cp "$tmp/delegation" "$tmp/payload/delegation"
tar -czf "$tmp/artifact.tar.gz" -C "$tmp/payload" delegation

case "$(uname -s)" in
  Linux) os=linux ;;
  Darwin) os=darwin ;;
  *) exit 1 ;;
esac
case "$(uname -m)" in
  x86_64|amd64) arch=amd64 ;;
  arm64|aarch64) arch=arm64 ;;
  *) exit 1 ;;
esac
artifact="delegation_0.1.0-alpha.0_${os}_${arch}.tar.gz"
if command -v sha256sum >/dev/null 2>&1; then
  checksum=$(sha256sum "$tmp/artifact.tar.gz" | awk '{ print $1 }')
else
  checksum=$(shasum -a 256 "$tmp/artifact.tar.gz" | awk '{ print $1 }')
fi
printf '%s  %s\n' "$checksum" "$artifact" >"$tmp/plugin/release-artifacts.sha256"

cat >"$tmp/fake-bin/curl" <<'EOF'
#!/bin/sh
set -eu
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--output" ]; then
    output=$2
    shift 2
    continue
  fi
  shift
done
cp "$DELEGATION_TEST_ARTIFACT" "$output"
EOF
chmod 0755 "$tmp/fake-bin/curl"

installed=$(PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$tmp/home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" "$tmp/plugin/scripts/install-runtime")
test "$installed" = "$tmp/home/bin/0.1.0-alpha.0/$os-$arch/delegation"
test -x "$installed"
"$installed" version --json >"$tmp/installed-version"
grep -F '"version":"0.1.0-alpha.0"' "$tmp/installed-version" >/dev/null

printf '%s\n' 'outside' >"$tmp/outside"
chmod 0644 "$tmp/outside"
rm "$tmp/payload/delegation"
ln -s "$tmp/outside" "$tmp/payload/delegation"
tar -czf "$tmp/malicious.tar.gz" -C "$tmp/payload" delegation
if command -v sha256sum >/dev/null 2>&1; then
  checksum=$(sha256sum "$tmp/malicious.tar.gz" | awk '{ print $1 }')
else
  checksum=$(shasum -a 256 "$tmp/malicious.tar.gz" | awk '{ print $1 }')
fi
printf '%s  %s\n' "$checksum" "$artifact" >"$tmp/plugin/release-artifacts.sha256"
if PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$tmp/malicious-home" DELEGATION_TEST_ARTIFACT="$tmp/malicious.tar.gz" "$tmp/plugin/scripts/install-runtime" >"$tmp/malicious-out" 2>"$tmp/malicious-err"; then
  printf '%s\n' 'expected symlink runtime archive to fail' >&2
  exit 1
fi
grep -F 'must contain one regular file' "$tmp/malicious-err" >/dev/null
test ! -x "$tmp/outside"
test ! -e "$tmp/malicious-home/bin/0.1.0-alpha.0/$os-$arch"
