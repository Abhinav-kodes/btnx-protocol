#!/usr/bin/env bash
set -euo pipefail

echo "🔍 Verifying BTNX Protocol setup"
echo "================================"
echo ""

# -------------------------------
# Toolchain checks
# -------------------------------

if command -v go >/dev/null 2>&1; then
    echo "✅ Go installed: $(go version)"
else
    echo "❌ Go not found"
    exit 1
fi

if command -v forge >/dev/null 2>&1; then
    echo "✅ Foundry installed: $(forge --version | head -n 1)"
else
    echo "❌ Foundry (forge) not found"
    exit 1
fi

if command -v cast >/dev/null 2>&1; then
    echo "✅ Cast available"
else
    echo "❌ Cast not found"
    exit 1
fi

# -------------------------------
# Project structure
# -------------------------------

echo ""
echo "📁 Checking project structure"

REQUIRED_DIRS=(
    "cmd"
    "pkg"
    "contracts"
    "scripts"
)

for dir in "${REQUIRED_DIRS[@]}"; do
    if [ -d "$dir" ]; then
        echo "  ✅ $dir/"
    else
        echo "  ❌ $dir/ missing"
        exit 1
    fi
done

if [ ! -f "contracts/foundry.toml" ]; then
    echo "  ❌ contracts/foundry.toml missing"
    exit 1
else
    echo "  ✅ contracts/foundry.toml found"
fi

# -------------------------------
# Solidity tests (ONLY protocol code)
# -------------------------------

echo ""
echo "🧪 Running Solidity tests (excluding dependencies)"

pushd contracts >/dev/null
forge test --no-match-path "lib"
popd >/dev/null

# -------------------------------
# Go sanity check
# -------------------------------

echo ""
echo "🐹 Running Go tests"

if find pkg -name "*.go" | grep -q .; then
    go test ./pkg/...
else
    echo "ℹ️  No Go packages yet (expected in early Phase 0)"
fi

# -------------------------------
# Done
# -------------------------------

echo ""
echo "🎉 BTNX setup verified successfully"
