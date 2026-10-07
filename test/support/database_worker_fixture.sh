#!/bin/sh
# Test-only single-file worker package for database_worker/package.test. The
# database:// resolver installs this file and execs it directly, so the package
# must be a local command: it re-execs a launchable worker command line.
#
# On subprocess lanes VGI_TEST_WORKER already is that command. The launcher lane
# prefixes it with launch:, a VGI resolver scheme rather than part of the
# worker's argv, so strip that. Lanes whose VGI_TEST_WORKER is a remote location
# (http://, unix://, oci://, ...) have no command to exec; they must set
# VGI_DATABASE_PACKAGE_WORKER to one (test/run_http_integration.sh and
# test/run_unix_integration.sh default it to the Python fixture worker).
worker_command=${VGI_DATABASE_PACKAGE_WORKER:-${VGI_TEST_WORKER#launch:}}
case "$worker_command" in
    "")
        echo "database_worker_fixture.sh: neither VGI_DATABASE_PACKAGE_WORKER nor VGI_TEST_WORKER is set" >&2
        exit 2
        ;;
    *://*)
        if [ -z "${VGI_DATABASE_PACKAGE_WORKER:-}" ]; then
            echo "database_worker_fixture.sh: VGI_TEST_WORKER is a location ('$worker_command'), not a command; set VGI_DATABASE_PACKAGE_WORKER to a launchable worker command for this lane" >&2
            exit 2
        fi
        ;;
esac
exec /bin/sh -c "$worker_command"
