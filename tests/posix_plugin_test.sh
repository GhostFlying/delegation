#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
plugin_root="$repo_root/plugins/delegation"
if [ "$(uname -s)" = Darwin ]; then
  tmp=$(mktemp -d /tmp/dp.XXXXXX)
else
  tmp=$(mktemp -d)
fi
service_pid=
cleanup() {
  if [ -n "$service_pid" ]; then
    kill "$service_pid" 2>/dev/null || true
    wait "$service_pid" 2>/dev/null || true
  fi
  rm -rf "$tmp"
}
trap cleanup EXIT HUP INT TERM
go_bin=${GO:-go}
"$go_bin" -C "$repo_root" build -trimpath -buildvcs=false -o "$tmp/delegation" ./cmd/delegation
unset DELEGATION_BINARY
unset DELEGATION_HOME

if DELEGATION_HOME="$tmp/missing" "$plugin_root/scripts/delegation-mcp" mcp root >"$tmp/out" 2>"$tmp/err"; then
  printf '%s\n' 'expected missing runtime launcher to fail' >&2
  exit 1
fi
grep -F 'runtime 0.1.0-alpha.0.m1 is not installed' "$tmp/err" >/dev/null

DELEGATION_BINARY="$tmp/delegation" "$plugin_root/scripts/delegation-mcp" version --json >"$tmp/version"
grep -F '"version":"0.1.0-alpha.0.m1"' "$tmp/version" >/dev/null

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
artifact="delegation_0.1.0-alpha.0.m1_${os}_${arch}.tar.gz"
if command -v sha256sum >/dev/null 2>&1; then
  checksum=$(sha256sum "$tmp/artifact.tar.gz" | awk '{ print $1 }')
else
  checksum=$(shasum -a 256 "$tmp/artifact.tar.gz" | awk '{ print $1 }')
fi
printf '%s  %s\n' "$checksum" "$artifact" >"$tmp/plugin/release-artifacts.sha256"

cat >"$tmp/fake-bin/curl" <<'EOF'
#!/bin/sh
set -eu
url=
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--output" ]; then
    output=$2
    shift 2
    continue
  fi
  case "$1" in
    --*) ;;
    *) url=$1 ;;
  esac
  shift
done
if [ "$url" != "$DELEGATION_TEST_EXPECTED_URL" ]; then
  printf '%s\n' "unexpected download URL: $url" >&2
  exit 1
fi
printf '%s\n' "$url" >>"$DELEGATION_TEST_DOWNLOAD_LOG"
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

expected_url="https://github.com/GhostFlying/delegation/releases/download/v0.1.0-alpha.0.m1/$artifact"
download_log="$tmp/downloads.log"
DELEGATION_TEST_EXPECTED_URL=$expected_url
DELEGATION_TEST_DOWNLOAD_LOG=$download_log
export DELEGATION_TEST_EXPECTED_URL DELEGATION_TEST_DOWNLOAD_LOG

cp -R "$tmp/plugin" "$tmp/missing-checksum-plugin"
printf '%s\n' '# intentionally empty for this test' >"$tmp/missing-checksum-plugin/release-artifacts.sha256"
: >"$download_log"
if PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$tmp/missing-checksum-home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" "$tmp/missing-checksum-plugin/scripts/install-runtime" >"$tmp/missing-checksum-out" 2>"$tmp/missing-checksum-err"; then
  printf '%s\n' 'expected a release without a pinned checksum to fail' >&2
  exit 1
fi
grep -F 'no pinned SHA-256' "$tmp/missing-checksum-err" >/dev/null
test ! -s "$download_log"

cp -R "$tmp/plugin" "$tmp/bad-checksum-plugin"
printf '%064d  %s\n' 0 "$artifact" >"$tmp/bad-checksum-plugin/release-artifacts.sha256"
: >"$download_log"
if PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$tmp/bad-checksum-home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" "$tmp/bad-checksum-plugin/scripts/install-runtime" >"$tmp/bad-checksum-out" 2>"$tmp/bad-checksum-err"; then
  printf '%s\n' 'expected an artifact with the wrong checksum to fail' >&2
  exit 1
fi
grep -F 'SHA-256 mismatch' "$tmp/bad-checksum-err" >/dev/null
test "$(wc -l <"$download_log")" -eq 1

cp -R "$tmp/plugin" "$tmp/version-plugin"
printf '%s\n' '9.9.9-test' >"$tmp/version-plugin/VERSION"
version_artifact="delegation_9.9.9-test_${os}_${arch}.tar.gz"
printf '%s  %s\n' "$checksum" "$version_artifact" >"$tmp/version-plugin/release-artifacts.sha256"
DELEGATION_TEST_EXPECTED_URL="https://github.com/GhostFlying/delegation/releases/download/v9.9.9-test/$version_artifact"
: >"$download_log"
if PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$tmp/version-home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" "$tmp/version-plugin/scripts/install-runtime" >"$tmp/version-out" 2>"$tmp/version-err"; then
  printf '%s\n' 'expected a downloaded runtime with the wrong version to fail' >&2
  exit 1
fi
grep -F 'downloaded runtime reports version' "$tmp/version-err" >/dev/null
test "$(wc -l <"$download_log")" -eq 1
DELEGATION_TEST_EXPECTED_URL=$expected_url

unsafe_home="$tmp/unsafe-home"
mkdir -p "$unsafe_home"
chmod 0755 "$unsafe_home"
: >"$download_log"
if PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$unsafe_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" "$tmp/plugin/scripts/install-runtime" >"$tmp/unsafe-home-out" 2>"$tmp/unsafe-home-err"; then
  printf '%s\n' 'expected an unsafe existing delegation home to fail' >&2
  exit 1
fi
grep -F 'mode 0700; refusing to modify existing permissions' "$tmp/unsafe-home-err" >/dev/null
case "$os" in
  linux) test "$(stat -c '%a' "$unsafe_home")" = 755 ;;
  darwin) test "$(stat -f '%Lp' "$unsafe_home")" = 755 ;;
esac
test ! -s "$download_log"
test ! -e "$unsafe_home/bin"

runtime_user_home="$tmp/runtime-user-home"
runtime_home="$runtime_user_home/.delegation"
umask 022
mkdir -p "$runtime_user_home"
: >"$download_log"
installed=$(PATH="$tmp/fake-bin:$PATH" HOME="$runtime_user_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" "$tmp/plugin/scripts/install-runtime")
test "$installed" = "$runtime_home/bin/0.1.0-alpha.0.m1/$os-$arch/delegation"
test -x "$installed"
test "$(wc -l <"$download_log")" -eq 1
case "$os" in
  linux) test "$(stat -c '%a' "$runtime_home")" = 700 ;;
  darwin) test "$(stat -f '%Lp' "$runtime_home")" = 700 ;;
esac
"$installed" version --json >"$tmp/installed-version"
grep -F '"version":"0.1.0-alpha.0.m1"' "$tmp/installed-version" >/dev/null
HOME="$runtime_user_home" "$tmp/plugin/scripts/delegation-mcp" version --json >"$tmp/launcher-installed-version"
grep -F '"version":"0.1.0-alpha.0.m1"' "$tmp/launcher-installed-version" >/dev/null
launcher="$tmp/plugin/scripts/delegation-mcp"
config="$runtime_home/config.json"
HOME="$runtime_user_home" "$launcher" setup controller \
  --controller-id 11111111-1111-4111-8111-111111111111 \
  --device-id 22222222-2222-4222-8222-222222222222 \
  --device-name acceptance-device \
  --broker-url ws://127.0.0.1:8787 \
  --auth-mode none \
  --json >"$tmp/launcher-setup"
grep -F '"role":"controller"' "$tmp/launcher-setup" >/dev/null
test -f "$config"
HOME="$runtime_user_home" "$launcher" doctor --json >"$tmp/launcher-doctor"
grep -F '"ok":true' "$tmp/launcher-doctor" >/dev/null
{
  printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"launcher-test","version":"1"}}}'
  printf '%s\n' '{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}'
  printf '%s\n' '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'
  sleep 1
} | DELEGATION_CONFIG="$config" HOME="$runtime_user_home" "$launcher" mcp root >"$tmp/launcher-mcp"
grep -F '"name":"list_devices"' "$tmp/launcher-mcp" >/dev/null
grep -F '"name":"describe_device"' "$tmp/launcher-mcp" >/dev/null
if [ "$os" = linux ]; then
  cat >"$tmp/fake-bin/systemctl" <<'EOF'
#!/bin/sh
set -eu
printf '%s\n' "$*" >>"$DELEGATION_TEST_SYSTEMCTL_LOG"
case " $* " in
  *" show "*)
    printf 'FragmentPath=%s/systemd/user/delegation.service\nDropInPaths=\n' "$XDG_CONFIG_HOME"
    ;;
esac
EOF
  chmod 0755 "$tmp/fake-bin/systemctl"
  service_config_home="$tmp/service-config"
  service_artifact="$service_config_home/systemd/user/delegation.service"
  service_log="$tmp/systemctl.log"
  mkdir -p "$service_config_home"
  HOME="$runtime_user_home" \
    "$launcher" service run --config "$config" \
    >"$tmp/launcher-service-run-out" 2>"$tmp/launcher-service-run-err" &
  service_pid=$!
  PATH="$tmp/fake-bin:$PATH" HOME="$runtime_user_home" XDG_CONFIG_HOME="$service_config_home" \
    DELEGATION_TEST_SYSTEMCTL_LOG="$service_log" \
    "$launcher" service install --config "$config" --json >"$tmp/launcher-service"
  grep -F '"state":"active"' "$tmp/launcher-service" >/dev/null
  grep -F '"kind":"systemdUser"' "$tmp/launcher-service" >/dev/null
  grep -F "\"artifact\":\"$service_artifact\"" "$tmp/launcher-service" >/dev/null
  test -f "$service_artifact"
  test "$(wc -l <"$service_log")" -eq 6
  kill "$service_pid"
  wait "$service_pid"
  service_pid=
fi
printf '%s\n' unexpected >"$(dirname "$installed")/unexpected.txt"
if PATH="$tmp/fake-bin:$PATH" HOME="$runtime_user_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" "$tmp/plugin/scripts/install-runtime" >"$tmp/existing-extra-out" 2>"$tmp/existing-extra-err"; then
  printf '%s\n' 'expected an installed directory with extra files to fail' >&2
  exit 1
fi
grep -F 'installed runtime directory contains unexpected files' "$tmp/existing-extra-err" >/dev/null
rm "$(dirname "$installed")/unexpected.txt"

race_home="$tmp/race-home"
race_target="$race_home/bin/0.1.0-alpha.0.m1/$os-$arch"
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
concurrent_binary="$concurrent_home/bin/0.1.0-alpha.0.m1/$os-$arch/delegation"
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
directory_race_binary="$directory_race_home/bin/0.1.0-alpha.0.m1/$os-$arch/delegation"
test -d "$directory_race_binary"
grep -F 'failed to publish runtime without replacing another file' "$tmp/directory-race-err" >/dev/null

symlink_race_home="$tmp/symlink-race-home"
symlink_race_outside="$tmp/symlink-race-outside"
if PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$symlink_race_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" DELEGATION_TEST_LINK_RACE=symlink DELEGATION_TEST_LINK_RACE_OUTSIDE="$symlink_race_outside" "$tmp/plugin/scripts/install-runtime" >"$tmp/symlink-race-out" 2>"$tmp/symlink-race-err"; then
  printf '%s\n' 'expected a symlink at the publication path to fail' >&2
  exit 1
fi
symlink_race_binary="$symlink_race_home/bin/0.1.0-alpha.0.m1/$os-$arch/delegation"
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
test ! -e "$tmp/malicious-home/bin/0.1.0-alpha.0.m1/$os-$arch"
