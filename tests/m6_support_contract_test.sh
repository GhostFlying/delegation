#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

require_literal() {
  file=$1
  text=$2
  grep -F "$text" "$repo_root/$file" >/dev/null
}

require_literal README.md '| Windows 11 | Codex | Supported |'
require_literal README.md '| Windows | TraeX | Unsupported |'
require_literal README.md 'Do not configure,'
require_literal README.md '[M6 release closure](docs/m6-release-closure.md)'

require_literal docs/m6-embedded-tailscale.md '| Windows 11 | Codex | Supported |'
require_literal docs/m6-embedded-tailscale.md '| Windows | TraeX | Unsupported |'
require_literal docs/m6-embedded-tailscale.md 'They do not block M6 release acceptance'
require_literal docs/m6-embedded-tailscale.md 'TraeX brokers, peers, or managed workers on Windows;'

require_literal plugins/delegation/skills/delegation-setup/SKILL.md \
  'M6 supports Codex and TraeX on Linux and macOS, and Codex on Windows 11.'
require_literal plugins/delegation/skills/delegation-setup/SKILL.md \
  'Windows TraeX is'
require_literal plugins/delegation/skills/delegation-setup/references/role-configuration.md \
  'Do not create a Windows TraeX broker, peer, managed home, or native service.'
require_literal plugins/delegation/skills/delegation-setup/references/native-services.md \
  'Windows TraeX is unsupported; do not configure or install it.'

require_literal docs/m6-release-closure.md \
  'Windows TraeX is a product-scope exclusion, not a successful qualification.'
require_literal docs/m6-release-closure.md \
  'ENGINEERING_VERDICT=BLOCKED'
require_literal docs/m6-release-closure.md \
  'The supported-platform technical acceptance verdict is `UNBLOCKED`'
require_literal docs/m6-release-closure.md \
  'the alpha.7 source commit'

printf '%s\n' 'M6_SUPPORT_CONTRACT_PASS'
