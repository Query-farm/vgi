#!/usr/bin/env bash
# Manual end-to-end check that Orchard sign-ins persist across processes and
# that rotation is shared safely between them, against the LIVE Front Desk.
# Not part of any CI lane. Linux (file backend) or macOS.
#
#   scripts/orchard_persist_e2e.sh [orchard-location] [renewal-token-file]
#
# Needs one fresh seeded Renewal Token in the file (default
# ~/orchard-e2e/ext-renewal.txt); it is spent by the run. Uses a private
# XDG_STATE_HOME so the user's own stored sign-ins are untouched, and
# VGI_OAUTH_LINUX_STORE=file so the Linux file backend is the one tested.
#
# Steps:
#   1. seed: process A signs in from the token file (test hook standing in for
#      the interactive sign-in) and saves the sign-in;
#   2. reuse: process B attaches with no token and no sign-in;
#   3. concurrent: 4 processes attach at once, each forcing a second refresh
#      (a spoiled first Pass), then
#   4. interleaved: two long-lived processes refresh on every query (Pass TTL
#      capped) at times chosen so each one's in-memory token was rotated by
#      the other more than 30 s earlier: without re-reading the store this
#      kills the family;
#   5. a final attach still works (the family survived);
#   6. invalid_grant: an old (rotated) record is put back; the next attach
#      deletes it and falls back to device code, which times out.
set -euo pipefail

LOCATION="${1:-orchard://orchard-e2e/hosted-python}"
TOKEN_FILE="${2:-$HOME/orchard-e2e/ext-renewal.txt}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CLI="${VGI_ORCHARD_CLI:-$ROOT/build/release/haybarn}"
[ -x "$CLI" ] || { echo "no CLI at $CLI" >&2; exit 2; }
[ -r "$TOKEN_FILE" ] || { echo "token file $TOKEN_FILE is not readable" >&2; exit 2; }

WORK="$(mktemp -d)"
chmod 700 "$WORK"
trap 'rm -rf "$WORK"' EXIT
export XDG_STATE_HOME="$WORK/state"
export VGI_OAUTH_LINUX_STORE=file
STORE="$XDG_STATE_HOME/queryfarm-vgi"
LOG="${ORCHARD_PERSIST_LOG:-$ROOT/orchard-persist-e2e.log}"
: > "$LOG"
CATALOG="${ORCHARD_E2E_CATALOG:-functions}"
ATTACH="ATTACH '$CATALOG' AS hp (TYPE vgi, LOCATION '$LOCATION');"

redact() { sed -E 's/ort1\.[A-Za-z0-9._~-]+/ort1.<redacted>/g; s/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/<jwt redacted>/g; s/(user_code=|Enter code: )[A-Z0-9-]+/\1<code>/g'; }
say() { echo "$*" | tee -a "$LOG"; }
fail() { say "FAIL: $*"; exit 1; }

# run NAME [env...] -- SQL on stdin. Output in $WORK/NAME.{out,err}.
run() {
	local name="$1"; shift
	env VGI_STDERR_LOG=1 "$@" "$CLI" -unsigned >"$WORK/$name.out" 2>"$WORK/$name.err" </dev/stdin
	local rc=$?
	{ echo "== $name (exit $rc)"; grep -E 'orchard\.|VGI Orchard|Error|Visit:|Enter code|saved sign-in' "$WORK/$name.err" | redact; } >> "$LOG"
	return $rc
}
claims_ok() { grep -q '"principal"' "$WORK/$1.out"; }
no_prompt() { ! grep -q 'Enter code' "$WORK/$1.err"; }

QUERY="SELECT hp.main.pass_claims(1) AS claims;"

say "== 1. seed"
printf '%s\n%s\n' "$ATTACH" "$QUERY" | run seed VGI_TEST_ORCHARD_SEED_TOKEN_FILE="$TOKEN_FILE" ||
	fail "seed attach failed"
claims_ok seed || fail "seed: no claims"
grep -q 'orchard.store saved' "$WORK/seed.err" || fail "seed: sign-in was not saved"
files=("$STORE"/*.cred)
[ "${#files[@]}" -eq 1 ] && [ -f "${files[0]}" ] || fail "expected one credential file in $STORE"
CRED="${files[0]}"
[ "$(stat -c %a "$STORE" 2>/dev/null || stat -f %Lp "$STORE")" = 700 ] || fail "store directory is not 0700"
[ "$(stat -c %a "$CRED" 2>/dev/null || stat -f %Lp "$CRED")" = 600 ] || fail "credential file is not 0600"
say "ok: saved to an owner-only file"

say "== 2. reuse in a new process (no token, no sign-in)"
printf '%s\n%s\n' "$ATTACH" "$QUERY" | run reuse || fail "reuse attach failed"
claims_ok reuse && no_prompt reuse || fail "reuse: needed a sign-in"
grep -q 'orchard.signin source=store' "$WORK/reuse.err" || fail "reuse: did not use the stored sign-in"
cp "$CRED" "$WORK/old-record" # a token that is rotated below (for step 6)
say "ok: second process signed in from the store"

say "== 3. four concurrent processes, each refreshing twice"
for i in 1 2 3 4; do
	( printf '%s\n%s\n' "$ATTACH" "$QUERY" | run "conc$i" VGI_TEST_ORCHARD_TAMPER_FIRST_PASS=1 ) &
done
wait
for i in 1 2 3 4; do
	claims_ok "conc$i" && no_prompt "conc$i" || fail "concurrent process $i failed"
	[ "$(grep -c 'orchard.refresh ok' "$WORK/conc$i.err")" -eq 2 ] || fail "concurrent process $i: expected 2 refreshes"
done
say "ok: 8 refreshes across 4 processes"

say "== 4. interleaved long-lived processes (about 80 s)"
# A Pass capped at 310 s is fresh for 10 s, then inside the 5-minute proactive
# window, so each later query refreshes.
# A: t=0 attach, t=40 query.  B: t=5 attach, t=50 query.
# A's in-memory token at t=40 was rotated by B at t=5 (35 s earlier).
( { echo "$ATTACH"; echo "$QUERY"; sleep 40; echo "$QUERY"; sleep 1; } | run ilA VGI_TEST_ORCHARD_PASS_TTL_SECONDS=310 ) &
sleep 5
( { echo "$ATTACH"; echo "$QUERY"; sleep 45; echo "$QUERY"; sleep 1; } | run ilB VGI_TEST_ORCHARD_PASS_TTL_SECONDS=310 ) &
wait
for p in ilA ilB; do
	[ "$(grep -c '"principal"' "$WORK/$p.out")" -eq 2 ] || fail "interleaved $p: a query failed"
	no_prompt "$p" || fail "interleaved $p: needed a sign-in"
done
grep -q 'orchard.store adopt_newer_token' "$WORK/ilA.err" || fail "interleaved A never adopted B's rotation"
say "ok: both processes kept working; A adopted B's rotated token"

say "== 5. the family survived"
printf '%s\n%s\n' "$ATTACH" "$QUERY" | run after || fail "attach after the rotations failed"
claims_ok after && no_prompt after || fail "after: needed a sign-in"
say "ok"

say "== 6. invalid_grant deletes the entry and falls back to device code"
sleep 31 # the old record's token was rotated long ago; presenting it now kills its family
cp "$WORK/old-record" "$CRED"
chmod 600 "$CRED"
printf 'SET vgi_oauth_timeout_seconds = 5;\n%s\n' "$ATTACH" | run dead || true
grep -q 'orchard.store deleted_after_invalid_grant' "$WORK/dead.err" || fail "dead: entry was not deleted"
grep -q 'Enter code' "$WORK/dead.err" || fail "dead: device code did not start"
grep -q 'saved sign-in for .* is no longer valid' "$WORK/dead.err" || fail "dead: no re-sign-in notice"
[ ! -e "$CRED" ] || fail "dead: credential file still exists"
say "ok: entry deleted, device code started, timed out"

if grep -q -E 'ort1\.[A-Za-z0-9]|eyJ[A-Za-z0-9_-]+\.eyJ' "$WORK"/*.out "$WORK"/*.err; then
	fail "a token appeared in the output"
fi
say "PASS (the token in $TOKEN_FILE is spent; reseed before the next run)"
