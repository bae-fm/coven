#!/bin/sh
# Installs scripts/hooks/* into the hooks dir every checkout of this repo
# shares (the common .git/hooks). Each installed hook runs the copy in the
# checkout git is working in, so a worktree runs its own branch's hooks rather
# than whichever checkout installed them.

set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HOOKS_DIR="$SCRIPT_DIR/hooks"
GIT_HOOKS_DIR="$(git -C "$SCRIPT_DIR" rev-parse --path-format=absolute --git-common-dir)/hooks"
mkdir -p "$GIT_HOOKS_DIR"

echo "Installing git hooks..."

for hook in "$HOOKS_DIR"/*; do
    [ -f "$hook" ] || continue
    hook_name=$(basename "$hook")
    target="$GIT_HOOKS_DIR/$hook_name"
    rm -f "$target"
    cat > "$target" <<EOF
#!/bin/sh
# Installed by scripts/install-hooks.sh: runs this checkout's own $hook_name.
hook="\$(git rev-parse --show-toplevel)/scripts/hooks/$hook_name"
[ -x "\$hook" ] || exit 0
exec "\$hook" "\$@"
EOF
    chmod +x "$target"
    echo "✅ Installed $hook_name"
done

echo ""
echo "Git hooks installed."
