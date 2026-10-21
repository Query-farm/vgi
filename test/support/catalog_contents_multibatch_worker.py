#!/usr/bin/env python3
"""Run the standard catalog fixtures with two-row metadata IPC batches.

Use as VGI_TEST_WORKER when running the existing catalog_contents integration
cases. Combined function/macro streams exercise dictionary replacements as
well as later rows and batches, without a separate catalog implementation.
"""

from functools import partial

from vgi.catalog import contents_codec

contents_codec.encode_collection = partial(contents_codec.encode_collection, batch_size=2)

if __name__ == "__main__":
    from vgi._test_fixtures.worker import main

    main()
