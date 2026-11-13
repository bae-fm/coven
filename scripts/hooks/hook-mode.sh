# Sourced by pre-commit and scripts/check-fast.sh. Bash 3.2 compatible.
#
# The pre-commit hook's mode: "strict" runs every step; "quick" runs the cheap
# steps and runs clippy over the changed crates and their dependents instead of
# the whole workspace. The mode is one machine-wide setting, `builds mode`
# (quick or strict; strict when unset, unreadable or without `builds`);
# HOOK_MODE overrides it, for testing the switch. Any other value fails, so a
# misspelled mode never silently runs as one of the two.
hook_mode() {
    local mode="${HOOK_MODE:-}"
    if [ -z "$mode" ]; then
        # The machine-wide setting; absent, unreadable or no `builds` is strict.
        mode=$(builds mode 2> /dev/null) || mode=strict
    fi
    case "$mode" in
        strict) echo strict ;;
        quick) echo quick ;;
        *)
            echo "HOOK_MODE must be 'quick' or 'strict', not '${mode}'" >&2
            return 1
            ;;
    esac
}
