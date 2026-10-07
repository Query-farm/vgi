"""Link the isolated I/O probe against an already built VGI archive and DuckDB host.

Avoids building another engine solely for a fixture executable. No downloads or
modifications to the supplied engine library or dependency prefix.
"""
import argparse
import json
from pathlib import Path
import re
import shlex
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('--build', type=Path, required=True)
parser.add_argument('--engine-library', type=Path, required=True)
args = parser.parse_args()
build = args.build.resolve()
engine = args.engine_library.resolve()
rows = json.loads((build / 'compile_commands.json').read_text())
row = next(row for row in rows if row['file'].endswith('/src/vgi_arrow_ipc.cpp'))
flags = shlex.split(row['command'])
flags = flags[:flags.index('-o')]
source = Path(row['file']).parent.parent / 'test/duckdb2/invocation_io.cpp'
archive = build / 'extension/vgi/libvgi_extension.a'
assert archive.is_file() and engine.is_file()
ninja = (build / 'build.ninja').read_text()
block = re.search(r'^build extension/vgi/vgi\.duckdb_extension:.*?(?=\n\n)', ninja, re.M | re.S).group()
line = re.search(r'^  LINK_LIBRARIES = (.*)$', block, re.M).group(1)
libraries = [item for item in shlex.split(line) if item != 'extension/vgi/libvgi_extension.a']
output = build / 'extension/vgi/vgi_invocation_io_test'
command = flags + [str(source), str(archive), str(engine)] + libraries + [
    '-pthread', '-Wl,--gc-sections', '-Wl,-rpath,' + str(engine.parent), '-o', str(output)]
subprocess.run(command, cwd=build, check=True)
print(output)
