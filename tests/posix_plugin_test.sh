#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
plugin_root="$repo_root/plugins/delegation"
version=$(sed -n '1p' "$plugin_root/VERSION")
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
"$go_bin" -C "$repo_root" build -tags=ts_omit_logtail -trimpath -buildvcs=false -o "$tmp/delegation" ./cmd/delegation
unset DELEGATION_BINARY
unset DELEGATION_HOME

DELEGATION_BINARY="$tmp/delegation" "$plugin_root/scripts/delegation-mcp" version --json >"$tmp/version"
grep -F "\"version\":\"$version\"" "$tmp/version" >/dev/null
ln -s "$tmp/delegation" "$tmp/delegation-link"
DELEGATION_BINARY="$tmp/delegation-link" "$plugin_root/scripts/delegation-mcp" version >"$tmp/override-link-out"
test "$(sed -n '1p' "$tmp/override-link-out")" = "$version"

cp -R "$plugin_root" "$tmp/plugin"
mkdir -p "$tmp/payload" "$tmp/fake-bin"
cp "$tmp/delegation" "$tmp/payload/delegation"
cp "$repo_root/THIRD_PARTY_NOTICES.txt" "$tmp/payload/THIRD_PARTY_NOTICES.txt"
tar -czf "$tmp/artifact.tar.gz" -C "$tmp/payload" delegation THIRD_PARTY_NOTICES.txt

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
artifact="delegation_${version}_${os}_${arch}.tar.gz"
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
  (umask 077 && mkdir -p "$DELEGATION_TEST_CREATE_TARGET")
fi
if [ -n "${DELEGATION_TEST_DIRECTORY_MODE_LOG:-}" ]; then
  staging=$(dirname "$output")
  case "$(uname -s)" in
    Linux) printf '%s %s\n' "$(stat -c '%a' "$staging")" "$(stat -c '%u' "$staging")" ;;
    Darwin) printf '%s %s\n' "$(stat -f '%Lp' "$staging")" "$(stat -f '%u' "$staging")" ;;
  esac >"$DELEGATION_TEST_DIRECTORY_MODE_LOG"
fi
EOF
chmod 0755 "$tmp/fake-bin/curl"
cat >"$tmp/fake-bin/link" <<'EOF'
#!/bin/sh
set -eu
case "$2" in
*/delegation) is_runtime=1 ;;
*) is_runtime=0 ;;
esac
if [ "$is_runtime" -eq 1 ] && [ -n "${DELEGATION_TEST_LINK_BARRIER_DIR:-}" ]; then
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
case "$is_runtime:${DELEGATION_TEST_LINK_RACE:-}" in
  1:directory)
    mkdir -p "$2"
    ;;
  1:symlink)
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
if [ "$is_runtime" -eq 1 ] && [ -n "${DELEGATION_TEST_LINK_BARRIER_DIR:-}" ]; then
  printf '%s\n' "$link_status" >"$DELEGATION_TEST_LINK_BARRIER_DIR/result.$$"
fi
exit "$link_status"
EOF
chmod 0755 "$tmp/fake-bin/link"

expected_url="https://github.com/GhostFlying/delegation/releases/download/v$version/$artifact"
download_log="$tmp/downloads.log"
DELEGATION_TEST_EXPECTED_URL=$expected_url
DELEGATION_TEST_DOWNLOAD_LOG=$download_log
export DELEGATION_TEST_EXPECTED_URL DELEGATION_TEST_DOWNLOAD_LOG

cold_home="$tmp/cold-home"
: >"$download_log"
PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$cold_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" "$tmp/plugin/scripts/delegation-mcp" version --json >"$tmp/cold-version"
grep -F "\"version\":\"$version\"" "$tmp/cold-version" >/dev/null
test "$(wc -l <"$download_log")" -eq 1
test -x "$cold_home/bin/$version/$os-$arch/delegation"

: >"$download_log"
PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$cold_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" "$tmp/plugin/scripts/delegation-mcp" version --json >"$tmp/warm-version"
grep -F "\"version\":\"$version\"" "$tmp/warm-version" >/dev/null
test ! -s "$download_log"

postcondition_plugin="$tmp/postcondition-plugin"
cp -R "$tmp/plugin" "$postcondition_plugin"
cat >"$postcondition_plugin/scripts/install-runtime" <<'EOF'
#!/bin/sh
exit 0
EOF
chmod 0755 "$postcondition_plugin/scripts/install-runtime"
if DELEGATION_HOME="$tmp/postcondition-home" "$postcondition_plugin/scripts/delegation-mcp" version >"$tmp/postcondition-out" 2>"$tmp/postcondition-err"; then
  printf '%s\n' 'expected launcher to reject a missing installer postcondition' >&2
  exit 1
fi
grep -F 'is not a regular executable' "$tmp/postcondition-err" >/dev/null

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

assert_directory_mode() {
  expected_mode=$1
  directory=$2
  case "$os" in
    linux) test "$(stat -c '%a' "$directory")" = "$expected_mode" ;;
    darwin) test "$(stat -f '%Lp' "$directory")" = "$expected_mode" ;;
  esac
}
assert_directory_owner() {
  directory=$1
  case "$os" in
    linux) test "$(stat -c '%u' "$directory")" = "$(id -u)" ;;
    darwin) test "$(stat -f '%u' "$directory")" = "$(id -u)" ;;
  esac
}

unsafe_bin_home="$tmp/unsafe-bin-home"
(umask 077 && mkdir "$unsafe_bin_home")
(umask 022 && mkdir "$unsafe_bin_home/bin")
printf '%s\n' preserved >"$unsafe_bin_home/bin/marker"
: >"$download_log"
if PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$unsafe_bin_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" "$tmp/plugin/scripts/install-runtime" >"$tmp/unsafe-bin-out" 2>"$tmp/unsafe-bin-err"; then
  printf '%s\n' 'expected an unsafe existing runtime bin directory to fail' >&2
  exit 1
fi
grep -F 'runtime bin directory must be owned by' "$tmp/unsafe-bin-err" >/dev/null
grep -F 'mode 0700; refusing to modify existing permissions' "$tmp/unsafe-bin-err" >/dev/null
assert_directory_mode 755 "$unsafe_bin_home/bin"
test "$(sed -n '1p' "$unsafe_bin_home/bin/marker")" = preserved
test ! -s "$download_log"

unsafe_version_home="$tmp/unsafe-version-home"
(umask 077 && mkdir -p "$unsafe_version_home/bin")
(umask 022 && mkdir "$unsafe_version_home/bin/$version")
printf '%s\n' preserved >"$unsafe_version_home/bin/$version/marker"
: >"$download_log"
if PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$unsafe_version_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" "$tmp/plugin/scripts/install-runtime" >"$tmp/unsafe-version-out" 2>"$tmp/unsafe-version-err"; then
  printf '%s\n' 'expected an unsafe existing runtime version directory to fail' >&2
  exit 1
fi
grep -F 'runtime version directory must be owned by' "$tmp/unsafe-version-err" >/dev/null
grep -F 'mode 0700; refusing to modify existing permissions' "$tmp/unsafe-version-err" >/dev/null
assert_directory_mode 755 "$unsafe_version_home/bin/$version"
test "$(sed -n '1p' "$unsafe_version_home/bin/$version/marker")" = preserved
test ! -s "$download_log"

unsafe_platform_home="$tmp/unsafe-platform-home"
(umask 077 && mkdir -p "$unsafe_platform_home/bin/$version")
(umask 022 && mkdir "$unsafe_platform_home/bin/$version/$os-$arch")
printf '%s\n' preserved >"$unsafe_platform_home/bin/$version/$os-$arch/marker"
: >"$download_log"
if PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$unsafe_platform_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" "$tmp/plugin/scripts/install-runtime" >"$tmp/unsafe-platform-out" 2>"$tmp/unsafe-platform-err"; then
  printf '%s\n' 'expected an unsafe existing runtime platform directory to fail' >&2
  exit 1
fi
grep -F 'runtime platform directory must be owned by' "$tmp/unsafe-platform-err" >/dev/null
grep -F 'mode 0700; refusing to modify existing permissions' "$tmp/unsafe-platform-err" >/dev/null
assert_directory_mode 755 "$unsafe_platform_home/bin/$version/$os-$arch"
test "$(sed -n '1p' "$unsafe_platform_home/bin/$version/$os-$arch/marker")" = preserved
test ! -s "$download_log"

runtime_user_home="$tmp/runtime-user-home"
runtime_home="$runtime_user_home/.delegation"
directory_mode_log="$tmp/runtime-directory-mode.log"
umask 022
mkdir -p "$runtime_user_home"
: >"$download_log"
installed=$(PATH="$tmp/fake-bin:$PATH" HOME="$runtime_user_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" DELEGATION_TEST_DIRECTORY_MODE_LOG="$directory_mode_log" "$tmp/plugin/scripts/install-runtime")
test "$installed" = "$runtime_home/bin/$version/$os-$arch/delegation"
test -x "$installed"
test -f "$(dirname "$installed")/THIRD_PARTY_NOTICES.txt"
cmp "$repo_root/THIRD_PARTY_NOTICES.txt" "$(dirname "$installed")/THIRD_PARTY_NOTICES.txt"
test "$(LC_ALL=C ls -A1 "$(dirname "$installed")")" = "THIRD_PARTY_NOTICES.txt
delegation"
test "$(wc -l <"$download_log")" -eq 1
case "$os" in
  linux)
    test "$(stat -c '%a' "$runtime_home")" = 700
    test "$(stat -c '%a' "$runtime_home/bin")" = 700
    test "$(stat -c '%a' "$runtime_home/bin/$version")" = 700
    test "$(stat -c '%a' "$(dirname "$installed")")" = 700
    ;;
  darwin)
    test "$(stat -f '%Lp' "$runtime_home")" = 700
    test "$(stat -f '%Lp' "$runtime_home/bin")" = 700
    test "$(stat -f '%Lp' "$runtime_home/bin/$version")" = 700
    test "$(stat -f '%Lp' "$(dirname "$installed")")" = 700
    ;;
esac
assert_directory_owner "$runtime_home"
assert_directory_owner "$runtime_home/bin"
assert_directory_owner "$runtime_home/bin/$version"
assert_directory_owner "$(dirname "$installed")"
test "$(sed -n '1p' "$directory_mode_log")" = "700 $(id -u)"
"$installed" version --json >"$tmp/installed-version"
grep -F "\"version\":\"$version\"" "$tmp/installed-version" >/dev/null
HOME="$runtime_user_home" "$tmp/plugin/scripts/delegation-mcp" version --json >"$tmp/launcher-installed-version"
grep -F "\"version\":\"$version\"" "$tmp/launcher-installed-version" >/dev/null
: >"$download_log"
chmod 0644 "$installed"
if PATH="$tmp/fake-bin:$PATH" HOME="$runtime_user_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" "$tmp/plugin/scripts/delegation-mcp" version >"$tmp/tampered-out" 2>"$tmp/tampered-err"; then
  printf '%s\n' 'expected launcher to reject a non-executable installed runtime' >&2
  exit 1
fi
grep -F 'is not a regular executable' "$tmp/tampered-err" >/dev/null
test ! -s "$download_log"
chmod 0755 "$installed"
launcher="$tmp/plugin/scripts/delegation-mcp"
config="$runtime_home/peer.json"
service_environment="$runtime_home/peer.env"
HOME="$runtime_user_home" "$launcher" setup peer \
  --controller-id 11111111-1111-4111-8111-111111111111 \
  --device-id 22222222-2222-4222-8222-222222222222 \
  --device-name acceptance-device \
  --broker-url ws://127.0.0.1:8787 \
  --auth-mode none \
  --json >"$tmp/launcher-setup"
grep -F '"role":"peer"' "$tmp/launcher-setup" >/dev/null
test -f "$config"
cat >"$service_environment" <<'EOF'
DELEGATION_CODEX_CONFIG_JSON={"model":"test","model_provider":"gateway","model_providers.gateway":{"name":"Gateway","base_url":"https://gateway.example.test/v1","wire_api":"responses","requires_openai_auth":false,"env_key":"GATEWAY_KEY"}}
GATEWAY_KEY=fixture-credential
EOF
chmod 0600 "$service_environment"
HOME="$runtime_user_home" "$launcher" doctor --config "$config" --json >"$tmp/launcher-doctor"
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
    printf 'FragmentPath=%s/systemd/user/delegation-peer.service\nDropInPaths=\n' "$XDG_CONFIG_HOME"
    ;;
esac
EOF
  chmod 0755 "$tmp/fake-bin/systemctl"
  service_config_home="$tmp/service-config"
  service_artifact="$service_config_home/systemd/user/delegation-peer.service"
  service_log="$tmp/systemctl.log"
  mkdir -p "$service_config_home"
  HOME="$runtime_user_home" \
    "$launcher" service run --config "$config" --environment-file "$service_environment" \
    >"$tmp/launcher-service-run-out" 2>"$tmp/launcher-service-run-err" &
  service_pid=$!
  PATH="$tmp/fake-bin:$PATH" HOME="$runtime_user_home" XDG_CONFIG_HOME="$service_config_home" \
    DELEGATION_TEST_SYSTEMCTL_LOG="$service_log" \
    "$launcher" service install --config "$config" --environment-file "$service_environment" \
      --json >"$tmp/launcher-service"
  grep -F '"state":"active"' "$tmp/launcher-service" >/dev/null
  grep -F '"kind":"systemdUser"' "$tmp/launcher-service" >/dev/null
  grep -F "\"artifact\":\"$service_artifact\"" "$tmp/launcher-service" >/dev/null
  grep -F "\"environmentFile\":\"$service_environment\"" "$tmp/launcher-service" >/dev/null
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
race_target="$race_home/bin/$version/$os-$arch"
installed=$(PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$race_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" DELEGATION_TEST_CREATE_TARGET="$race_target" "$tmp/plugin/scripts/install-runtime")
test "$installed" = "$race_target/delegation"
test -x "$installed"
test -f "$race_target/THIRD_PARTY_NOTICES.txt"
test "$(LC_ALL=C ls -A1 "$race_target")" = "THIRD_PARTY_NOTICES.txt
delegation"

concurrent_home="$tmp/concurrent-home"
concurrent_barrier="$tmp/concurrent-barrier"
PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$concurrent_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" DELEGATION_TEST_LINK_BARRIER_DIR="$concurrent_barrier" "$tmp/plugin/scripts/install-runtime" >"$tmp/concurrent-first" 2>"$tmp/concurrent-first-err" &
first_pid=$!
PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$concurrent_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" DELEGATION_TEST_LINK_BARRIER_DIR="$concurrent_barrier" "$tmp/plugin/scripts/install-runtime" >"$tmp/concurrent-second" 2>"$tmp/concurrent-second-err" &
second_pid=$!
wait "$first_pid"
wait "$second_pid"
concurrent_binary="$concurrent_home/bin/$version/$os-$arch/delegation"
test "$(sed -n '1p' "$tmp/concurrent-first")" = "$concurrent_binary"
test "$(sed -n '1p' "$tmp/concurrent-second")" = "$concurrent_binary"
test -x "$concurrent_binary"
test -f "$(dirname "$concurrent_binary")/THIRD_PARTY_NOTICES.txt"
test "$(LC_ALL=C ls -A1 "$(dirname "$concurrent_binary")")" = "THIRD_PARTY_NOTICES.txt
delegation"
test "$(find "$concurrent_barrier" -type f -name 'result.*' | wc -l)" -eq 2
test "$(grep -l '^0$' "$concurrent_barrier"/result.* | wc -l)" -eq 1
test "$(grep -L '^0$' "$concurrent_barrier"/result.* | wc -l)" -eq 1

launcher_concurrent_home="$tmp/launcher-concurrent-home"
launcher_concurrent_barrier="$tmp/launcher-concurrent-barrier"
: >"$download_log"
PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$launcher_concurrent_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" DELEGATION_TEST_LINK_BARRIER_DIR="$launcher_concurrent_barrier" "$tmp/plugin/scripts/delegation-mcp" version >"$tmp/launcher-concurrent-first" 2>"$tmp/launcher-concurrent-first-err" &
first_pid=$!
PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$launcher_concurrent_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" DELEGATION_TEST_LINK_BARRIER_DIR="$launcher_concurrent_barrier" "$tmp/plugin/scripts/delegation-mcp" version >"$tmp/launcher-concurrent-second" 2>"$tmp/launcher-concurrent-second-err" &
second_pid=$!
wait "$first_pid"
wait "$second_pid"
test "$(sed -n '1p' "$tmp/launcher-concurrent-first")" = "$version"
test "$(sed -n '1p' "$tmp/launcher-concurrent-second")" = "$version"
test -x "$launcher_concurrent_home/bin/$version/$os-$arch/delegation"
test "$(wc -l <"$download_log")" -eq 2

directory_race_home="$tmp/directory-race-home"
if PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$directory_race_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" DELEGATION_TEST_LINK_RACE=directory "$tmp/plugin/scripts/install-runtime" >"$tmp/directory-race-out" 2>"$tmp/directory-race-err"; then
  printf '%s\n' 'expected a directory at the publication path to fail' >&2
  exit 1
fi
directory_race_binary="$directory_race_home/bin/$version/$os-$arch/delegation"
test -d "$directory_race_binary"
grep -F 'failed to publish runtime without replacing another file' "$tmp/directory-race-err" >/dev/null

symlink_race_home="$tmp/symlink-race-home"
symlink_race_outside="$tmp/symlink-race-outside"
if PATH="$tmp/fake-bin:$PATH" DELEGATION_HOME="$symlink_race_home" DELEGATION_TEST_ARTIFACT="$tmp/artifact.tar.gz" DELEGATION_TEST_LINK_RACE=symlink DELEGATION_TEST_LINK_RACE_OUTSIDE="$symlink_race_outside" "$tmp/plugin/scripts/install-runtime" >"$tmp/symlink-race-out" 2>"$tmp/symlink-race-err"; then
  printf '%s\n' 'expected a symlink at the publication path to fail' >&2
  exit 1
fi
symlink_race_binary="$symlink_race_home/bin/$version/$os-$arch/delegation"
test -L "$symlink_race_binary"
test -z "$(LC_ALL=C ls -A1 "$symlink_race_outside")"
grep -F 'failed to publish runtime without replacing another file' "$tmp/symlink-race-err" >/dev/null

printf '%s\n' 'outside' >"$tmp/outside"
chmod 0644 "$tmp/outside"
rm "$tmp/payload/delegation"
ln -s "$tmp/outside" "$tmp/payload/delegation"
tar -czf "$tmp/malicious.tar.gz" -C "$tmp/payload" delegation THIRD_PARTY_NOTICES.txt
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
grep -F 'must contain two regular files' "$tmp/malicious-err" >/dev/null
test ! -x "$tmp/outside"
test ! -e "$tmp/malicious-home/bin/$version/$os-$arch"
