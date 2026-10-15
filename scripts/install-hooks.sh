#!/usr/bin/env bash
# Configure Git to use the repository's tracked .githooks/ directory so
# scripts/local-gates.sh runs automatically before every git push.
set -euo pipefail

cd "$(dirname "$0")/.."
chmod +x scripts/local-gates.sh .githooks/pre-push
git config core.hooksPath .githooks
echo "Configured git core.hooksPath -> .githooks (pre-push runs scripts/local-gates.sh)"
