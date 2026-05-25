#!/bin/sh
# Install git hooks from scripts/hooks into .git/hooks as symlinks.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HOOKS_DIR="$SCRIPT_DIR/hooks"
GIT_HOOKS_DIR="$SCRIPT_DIR/../.git/hooks"

echo "Installing git hooks..."

for hook in "$HOOKS_DIR"/*; do
    if [ -f "$hook" ]; then
        hook_name=$(basename "$hook")
        target="$GIT_HOOKS_DIR/$hook_name"
        rm -f "$target"
        ln -s "$hook" "$target"
        echo "✅ Installed $hook_name (symlink)"
    fi
done

echo ""
echo "Git hooks installed."
