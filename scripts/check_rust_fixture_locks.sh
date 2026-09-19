#!/usr/bin/env bash
# Refuse a pair of Rust fixture lockfiles that would link two different
# vgi-rpcs, or anything but a crates.io one, into vgi_unit_tests.
#
# vgi_unit_tests links two staticlibs built out-of-band (make unit_test_rust_libs):
#   test/support/sabffi   — a bare vgi-rpc server; depends on vgi-rpc directly.
#   test/support/sabtable — a vgi SDK worker; takes vgi-rpc through `vgi`.
# Each has its own Cargo.lock, so nothing in Cargo keeps them on one vgi-rpc:
# bump sabtable's `vgi` to an SDK minor that moved vgi-rpc and sabffi keeps
# building against the old one. This check is what keeps them in step.
#
# It also refuses a path/git source for vgi or vgi-rpc. Those used to point at
# sibling checkouts (../vgi-rust, ../vgi-rpc-rust), which made the build depend
# on whatever branch each sibling was on and silently rewrote the lockfiles
# whenever a sibling's version differed.
#
# Reads the committed lockfiles only (no cargo, no network), so it runs before
# the build and fails in milliseconds.
set -euo pipefail

cd "$(dirname "$0")/.."

CRATES_IO="registry+https://github.com/rust-lang/crates.io-index"
SABFFI_LOCK=test/support/sabffi/Cargo.lock
SABTABLE_LOCK=test/support/sabtable/Cargo.lock

# Print "<version> <source>" for each [[package]] named $2 in lockfile $1.
# A package with no `source` line is a path dependency; print "path" for it.
lock_entries() {
	awk -v want="$2" '
		function flush() {
			if (name == want) print version, (source == "" ? "path" : source)
			name = ""; version = ""; source = ""
		}
		/^\[\[package\]\]$/ { flush(); next }
		/^name = /    { name = $3;    gsub(/"/, "", name) }
		/^version = / { version = $3; gsub(/"/, "", version) }
		/^source = /  { source = $3;  gsub(/"/, "", source) }
		END { flush() }
	' "$1"
}

failed=0
fail() {
	echo "check_rust_fixture_locks: $*" >&2
	failed=1
}

# Set REPLY to the single crates.io version of crate $2 in lockfile $1, or
# report why there is not exactly one and leave REPLY empty. (Not called in a
# command substitution: fail() must set `failed` in this shell.)
sole_registry_version() {
	local lock="$1" crate="$2" entries count version source
	REPLY=""
	entries="$(lock_entries "$lock" "$crate")"
	count="$(printf '%s' "$entries" | grep -c . || true)"
	if [ "$count" -ne 1 ]; then
		fail "$lock resolves $count copies of $crate, expected exactly 1:"
		printf '%s\n' "$entries" | sed 's/^/    /' >&2
		return 0
	fi
	read -r version source <<<"$entries"
	if [ "$source" != "$CRATES_IO" ]; then
		fail "$lock takes $crate $version from '$source', not crates.io." \
			"Remove the path/git dependency or [patch.crates-io] (keep a local-checkout patch uncommitted)."
		return 0
	fi
	REPLY="$version"
}

sole_registry_version "$SABFFI_LOCK" vgi-rpc
sabffi_rpc="$REPLY"
sole_registry_version "$SABTABLE_LOCK" vgi-rpc
sabtable_rpc="$REPLY"
sole_registry_version "$SABTABLE_LOCK" vgi
sabtable_vgi="$REPLY"

if [ -n "$sabffi_rpc" ] && [ -n "$sabtable_rpc" ] && [ "$sabffi_rpc" != "$sabtable_rpc" ]; then
	fail "the two fixtures resolve different vgi-rpcs:" \
		"sabtable gets $sabtable_rpc through vgi ${sabtable_vgi:-?}, sabffi pins $sabffi_rpc." \
		"Move sabffi's vgi-rpc requirement (test/support/sabffi/Cargo.toml) to $sabtable_rpc and run" \
		"'cargo update -p vgi-rpc' there, so both lockfiles name one vgi-rpc."
fi

if [ "$failed" -ne 0 ]; then
	exit 1
fi
echo "check_rust_fixture_locks: sabffi and sabtable both resolve vgi-rpc $sabffi_rpc from crates.io (sabtable via vgi $sabtable_vgi)"
