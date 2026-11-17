#!/usr/bin/env bash
# Manual end-to-end check of orchard:// LOCATIONs against the LIVE Orchard
# Front Desk and a live hosted worker. Not part of any CI lane.
#
#   scripts/orchard_e2e.sh [orchard-location] [renewal-token-file]
#
# Defaults: orchard://orchard-e2e/hosted-python and ~/orchard-e2e/ext-renewal.txt.
# The token file holds one seeded Renewal Token (ort1.…). Every refresh rotates
# it and the extension keeps the newest only in memory, so the file is spent
# after one run: reseed it before running again.
#
# The token never appears on a command line: it is written into a mode-600 SQL
# file that is deleted on exit. Output is redacted before it is printed.
#
# Checks:
#   1. ATTACH with oauth_refresh_token signs in and routes to the Field Address.
#   2. pass_claims(1) returns the verified Pass claims (iss/aud checked by the worker).
#   3. With VGI_TEST_ORCHARD_TAMPER_FIRST_PASS=1 the first RPC draws a 401, the
#      extension refreshes once and retries, and the query still succeeds.
set -euo pipefail

LOCATION="${1:-orchard://orchard-e2e/hosted-python}"
TOKEN_FILE="${2:-$HOME/orchard-e2e/ext-renewal.txt}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CLI="${VGI_ORCHARD_CLI:-$ROOT/build/release/haybarn}"
[ -x "$CLI" ] || CLI="$ROOT/build/release/duckdb"
[ -x "$CLI" ] || { echo "no CLI under $ROOT/build/release" >&2; exit 2; }
[ -r "$TOKEN_FILE" ] || { echo "token file $TOKEN_FILE is not readable" >&2; exit 2; }

WORK="$(mktemp -d)"
chmod 700 "$WORK"
trap 'rm -rf "$WORK"' EXIT
SQL="$WORK/e2e.sql"
LOG="${ORCHARD_E2E_LOG:-$ROOT/orchard-e2e.log}"

umask 077
{
	# The hosted fixture worker's catalog is named `functions`. The bare form,
	# ATTACH 'orchard://…' AS hp (TYPE vgi, …), discovers it instead.
	printf "ATTACH '%s' AS hp (TYPE vgi, LOCATION '%s', oauth_refresh_token '" "${ORCHARD_E2E_CATALOG:-functions}" "$LOCATION"
	tr -d '\r\n' < "$TOKEN_FILE"
	printf "');\n"
	cat <<'SQL'
.mode line
SELECT database_name, options['location'] AS location, options['oauth_refresh_token'] AS token_option
FROM duckdb_databases() WHERE database_name = 'hp';
SELECT hp.main.pass_claims(1) AS first_call;
SELECT hp.main.pass_claims(2) AS second_call;
SELECT catalog_name, origin, status, has_refresh_token, expires_in
FROM vgi_oauth_tokens();
SQL
} > "$SQL"

redact() { sed -E 's/ort1\.[A-Za-z0-9._~-]+/ort1.<redacted>/g; s/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/<jwt redacted>/g'; }

set +e
VGI_STDERR_LOG=1 VGI_TEST_ORCHARD_TAMPER_FIRST_PASS=1 "$CLI" -unsigned -f "$SQL" 2>"$WORK/stderr" >"$WORK/stdout"
status=$?
set -e
{
	echo "== stdout"; redact < "$WORK/stdout"
	echo "== stderr (orchard / 401 lines)"; grep -E 'orchard\.|401_received|VGI Orchard' "$WORK/stderr" | redact || true
	echo "== exit $status"
} | tee "$LOG"

fail() {
	{ echo "== stderr tail"; grep -v -E '^[0-9T:.-]+ \[VGI\] (catalog|http)\.' "$WORK/stderr" | tail -30 | redact; echo "FAIL: $*"; } | tee -a "$LOG"
	exit 1
}
[ "$status" -eq 0 ] || fail "CLI exited $status"
grep -q 'first_call = .*"principal"' "$WORK/stdout" || fail "pass_claims did not return claims"
grep -q 'second_call = .*"principal"' "$WORK/stdout" || fail "second pass_claims call failed"
grep -q 'location = orchard://' "$WORK/stdout" || fail "LOCATION is not shown as the orchard:// string"
grep -q 'token_option = <redacted>' "$WORK/stdout" || fail "oauth_refresh_token is not redacted"
refreshes=$(grep -c 'orchard.refresh ok' "$WORK/stderr" || true)
rejected=$(grep -c 'handler=rejected_token' "$WORK/stderr" || true)
[ "$refreshes" -eq 2 ] || fail "expected 2 refreshes (sign-in + one 401), saw $refreshes"
[ "$rejected" -eq 1 ] || fail "expected exactly one 401 handled, saw $rejected"
if grep -q -E 'ort1\.[A-Za-z0-9]|eyJ[A-Za-z0-9_-]+\.eyJ' "$WORK/stdout" "$WORK/stderr"; then
	fail "a token appeared in the output"
fi
echo "PASS: $LOCATION (token in $TOKEN_FILE is now spent; reseed before the next run)" | tee -a "$LOG"
