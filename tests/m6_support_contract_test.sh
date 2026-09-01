#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

require_literal() {
  file=$1
  text=$2
  grep -F "$text" "$repo_root/$file" >/dev/null
}

reject_literal() {
  file=$1
  text=$2
  if grep -F "$text" "$repo_root/$file" >/dev/null; then
    printf '%s\n' "unexpected contract text in $file: $text" >&2
    exit 1
  fi
}

for file in README.md docs/m6-embedded-tailscale.md docs/m6-release-closure.md; do
  require_literal "$file" '| Linux | Codex | Supported |'
  require_literal "$file" '| Linux | TraeX | Supported |'
  require_literal "$file" '| macOS | Codex | Supported |'
  require_literal "$file" '| macOS | TraeX | Supported |'
  require_literal "$file" '| Windows 11 | Codex | Supported |'
  require_literal "$file" '| Windows | TraeX | Unsupported |'
done

require_literal README.md 'Do not configure,'
require_literal README.md '[M6 release closure](docs/m6-release-closure.md)'
require_literal README.md 'The first move from alpha.4 requires explicit local bootstrap'
require_literal README.md 'After every online peer and the broker run a coordination-capable version'
require_literal README.md 'The broker freezes the online, compatible, upgrade-capable peers'
require_literal README.md '`controllerUpgrade` transaction'

require_literal docs/m6-embedded-tailscale.md 'They do not block M6 release acceptance'
require_literal docs/m6-embedded-tailscale.md 'TraeX brokers, peers, or managed workers on Windows;'
require_literal docs/m6-embedded-tailscale.md 'through the forward-only local upgrade documented in `README.md`'
require_literal docs/m6-embedded-tailscale.md 'rollback after durable'

require_literal plugins/delegation/skills/delegation-setup/SKILL.md \
  'Windows TraeX is unsupported.'
require_literal plugins/delegation/skills/delegation-setup/SKILL.md \
  'M6 supports Codex and TraeX on Linux and macOS, and Codex on'
require_literal plugins/delegation/skills/delegation-setup/SKILL.md \
  'Windows 11. Stop instead of configuring, qualifying, or installing a Windows TraeX deployment.'
require_literal plugins/delegation/skills/delegation-setup/SKILL.md \
  'Before installing a fresh user service'
require_literal plugins/delegation/skills/delegation-setup/SKILL.md \
  'The first move from alpha.4 requires `service upgrade ... --bootstrap`'
require_literal plugins/delegation/skills/delegation-setup/SKILL.md \
  'invoke `service upgrade` without `--bootstrap` against the'
require_literal plugins/delegation/skills/delegation-setup/references/role-configuration.md \
  'Do not create a Windows TraeX broker, peer, managed home, or native service.'
require_literal plugins/delegation/skills/delegation-setup/references/native-services.md \
  'Windows TraeX is unsupported; do not configure or install it.'
require_literal plugins/delegation/skills/delegation-setup/references/native-services.md \
  'The first move from alpha.4 requires explicit local bootstrap authorization'
require_literal plugins/delegation/skills/delegation-setup/references/native-services.md \
  'Once every online peer and the broker run a coordination-capable version'
require_literal plugins/delegation/skills/delegation-setup/references/native-services.md \
  'broker listeners expose no upgrade API.'

require_literal docs/m6-release-closure.md \
  'Windows TraeX is a product-scope exclusion, not a successful qualification.'
require_literal docs/m6-release-closure.md \
  'ENGINEERING_VERDICT=BLOCKED'
require_literal docs/m6-release-closure.md \
  'The supported-platform technical acceptance verdict is `UNBLOCKED`'
require_literal docs/m6-release-closure.md \
  'broker-owned, peer-first and'
require_literal docs/m6-release-closure.md \
  'the alpha.7 source commit'

printf '%s\n' 'M6_SUPPORT_CONTRACT_PASS'
