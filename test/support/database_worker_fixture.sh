#!/bin/sh
# Test-only single-file worker package. The conformance runner already supplies
# VGI_TEST_WORKER; this wrapper lets database:// exercise direct exec while
# preserving whichever SDK worker command the lane selected. The launcher test
# lane prefixes its command with launch:, which is a VGI resolver scheme rather
# than part of the worker's argv, so strip it inside this direct-exec fixture.
worker_command=${VGI_TEST_WORKER#launch:}
exec /bin/sh -c "$worker_command"
