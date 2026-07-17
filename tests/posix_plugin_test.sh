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
"$go_bin" -C "$repo_root" build -trimpath -buildvcs=false -o "$tmp/delegation" ./cmd/delegation
unset DELEGATION_BINARY

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
if [ -n "${DELEGATION_TEST_CREATE_TARGET:-}" ]; then
  mkdir -p "$DELEGATION_TEST_CREATE_TARGET"
fi
EOF
chmod 0755 "$tmp/fake-bin/curl"
cat >"$tmp/fake-bin/link" <<'EOF'
#!/bin/sh
set -eu
if [ -n "${DELEGATION_TEST_LINK_BARRIER_DIR:-}" ]; then
  mkdir -p "$DELEGATION_TEST_LINK_BARRIER_DIR"
  : >"$DELEGATION_TEST_LINK_BARRIER_DIR/ready.$$"
  attempts=0
  while [ "$(find "$DELEGATION_TEST_LINK_BARRIER_DIR" -type f -name 'ready.*' | wc -l)" -lt 2 ]; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 10 ]; then
      printf '%s\n' 'timed out waiting for publication race peer' >&2
      exit 1
    fi
    sleep 1
  done
fi
case "${DELEGATION_TEST_LINK_RACE:-}" in
  directory)
    mkdir -p "$2"
    ;;
  symlink)
    mkdir -p "$DELEGATION_TEST_LINK_RACE_OUTSIDE"
    /bin/ln -s "$DELEGATION_TEST_LINK_RACE_OUTSIDE" "$2"
    ;;
esac
real_link=$(PATH=/usr/bin:/bin command -v link)
if "$real_link" "$@"; then
  link_status=0
else
  link_status=$?
fi
if [ -n "${DELEGATION_TEST_LINK_BARRIER_DIR:-}" ]; then
  printf '%s\n' "$link_status" >"$DELEGATION_TEST_LINK_BARRIER_DIR/result.$$"
fi
exit "$link_status"
EOF
chmod 0755 "$tmp/fake-bin/link"

mkdir -p "$tmp/home/.locks/install-0.1.0-alpha.0-$os-$arch"
installed=$(PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$tmp/home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" "$tmp/plugin/scripts/install-runtime")
test "$installed" = "$tmp/home/bin/0.1.0-alpha.0/$os-$arch/delegation"
test -x "$installed"
"$installed" version --json >"$tmp/installed-version"
grep -F '"version":"0.1.0-alpha.0"' "$tmp/installed-version" >/dev/null
printf '%s\n' unexpected >"$(dirname "$installed")/unexpected.txt"
if PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$tmp/home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" "$tmp/plugin/scripts/install-runtime" >"$tmp/existing-extra-out" 2>"$tmp/existing-extra-err"; then
  printf '%s\n' 'expected an installed directory with extra files to fail' >&2
  exit 1
fi
grep -F 'installed runtime directory contains unexpected files' "$tmp/existing-extra-err" >/dev/null
rm "$(dirname "$installed")/unexpected.txt"

race_home="$tmp/race-home"
race_target="$race_home/bin/0.1.0-alpha.0/$os-$arch"
installed=$(PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$race_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" DELEGATION_TEST_CREATE_TARGET="$race_target" "$tmp/plugin/scripts/install-runtime")
test "$installed" = "$race_target/delegation"
test -x "$installed"
test "$(LC_ALL=C ls -A1 "$race_target")" = delegation

concurrent_home="$tmp/concurrent-home"
concurrent_barrier="$tmp/concurrent-barrier"
PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$concurrent_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" DELEGATION_TEST_LINK_BARRIER_DIR="$concurrent_barrier" "$tmp/plugin/scripts/install-runtime" >"$tmp/concurrent-first" 2>"$tmp/concurrent-first-err" &
first_pid=$!
PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$concurrent_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" DELEGATION_TEST_LINK_BARRIER_DIR="$concurrent_barrier" "$tmp/plugin/scripts/install-runtime" >"$tmp/concurrent-second" 2>"$tmp/concurrent-second-err" &
second_pid=$!
wait "$first_pid"
wait "$second_pid"
concurrent_binary="$concurrent_home/bin/0.1.0-alpha.0/$os-$arch/delegation"
test "$(sed -n '1p' "$tmp/concurrent-first")" = "$concurrent_binary"
test "$(sed -n '1p' "$tmp/concurrent-second")" = "$concurrent_binary"
test -x "$concurrent_binary"
test "$(LC_ALL=C ls -A1 "$(dirname "$concurrent_binary")")" = delegation
test "$(find "$concurrent_barrier" -type f -name 'result.*' | wc -l)" -eq 2
test "$(grep -l '^0$' "$concurrent_barrier"/result.* | wc -l)" -eq 1
test "$(grep -L '^0$' "$concurrent_barrier"/result.* | wc -l)" -eq 1

directory_race_home="$tmp/directory-race-home"
if PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$directory_race_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" DELEGATION_TEST_LINK_RACE=directory "$tmp/plugin/scripts/install-runtime" >"$tmp/directory-race-out" 2>"$tmp/directory-race-err"; then
  printf '%s\n' 'expected a directory at the publication path to fail' >&2
  exit 1
fi
directory_race_binary="$directory_race_home/bin/0.1.0-alpha.0/$os-$arch/delegation"
test -d "$directory_race_binary"
grep -F 'failed to publish runtime without replacing another file' "$tmp/directory-race-err" >/dev/null

symlink_race_home="$tmp/symlink-race-home"
symlink_race_outside="$tmp/symlink-race-outside"
if PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$symlink_race_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" DELEGATION_TEST_LINK_RACE=symlink DELEGATION_TEST_LINK_RACE_OUTSIDE="$symlink_race_outside" "$tmp/plugin/scripts/install-runtime" >"$tmp/symlink-race-out" 2>"$tmp/symlink-race-err"; then
  printf '%s\n' 'expected a symlink at the publication path to fail' >&2
  exit 1
fi
symlink_race_binary="$symlink_race_home/bin/0.1.0-alpha.0/$os-$arch/delegation"
test -L "$symlink_race_binary"
test -z "$(LC_ALL=C ls -A1 "$symlink_race_outside")"
grep -F 'failed to publish runtime without replacing another file' "$tmp/symlink-race-err" >/dev/null

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
