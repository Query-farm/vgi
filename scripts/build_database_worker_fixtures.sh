#!/usr/bin/env bash
# Build real, self-contained database:// worker artifacts for the host platform.

set -euo pipefail

project_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
output_dir=${VGI_DATABASE_WORKER_FIXTURE_DIR:-"${project_dir}/build/database-worker-fixtures"}
python_dir=${VGI_DATABASE_PYTHON_DIR:-"${HOME}/Development/vgi-python"}
rust_dir=${VGI_DATABASE_RUST_DIR:-"${HOME}/Development/vgi-rust"}
open_meteo_dir=${VGI_DATABASE_OPEN_METEO_DIR:-"${HOME}/Development/vgi-open-meteo"}

for command_name in uv bun cargo tar; do
	if ! command -v "${command_name}" >/dev/null 2>&1; then
		echo "database-worker fixtures require ${command_name} on PATH" >&2
		exit 1
	fi
done
for source_dir in "${python_dir}" "${rust_dir}" "${open_meteo_dir}"; do
	if [[ ! -d "${source_dir}" ]]; then
		echo "database-worker fixture source directory does not exist: ${source_dir}" >&2
		exit 1
	fi
done

mkdir -p "${output_dir}"
scratch_dir=$(mktemp -d "${TMPDIR:-/tmp}/vgi-database-workers.XXXXXX")
cleanup() {
	rm -rf "${scratch_dir}"
}
trap cleanup EXIT

echo "Building frozen Python worker"
uv run --project "${python_dir}" --locked --with pyinstaller==6.22.2 \
	pyinstaller --clean --onefile --name vgi-python-worker \
	--distpath "${output_dir}" \
	--workpath "${scratch_dir}/pyinstaller-build" \
	--specpath "${scratch_dir}/pyinstaller-spec" \
	"${project_dir}/test/support/database_workers/python_worker.py"

if [[ -n "${VGI_BUN_TARGET:-}" ]]; then
	bun_target=${VGI_BUN_TARGET}
else
	host_os=$(uname -s)
	host_arch=$(uname -m)
	case "${host_os}/${host_arch}" in
		Darwin/arm64) bun_target=bun-darwin-arm64 ;;
		Darwin/x86_64) bun_target=bun-darwin-x64 ;;
		Linux/aarch64|Linux/arm64) bun_target=bun-linux-arm64 ;;
		Linux/x86_64) bun_target=bun-linux-x64-baseline ;;
		*)
			echo "cannot infer a Bun executable target for ${host_os}/${host_arch}" >&2
			exit 1
			;;
	esac
fi

echo "Building Open Meteo worker for ${bun_target}"
(
	cd "${open_meteo_dir}"
	bun install --frozen-lockfile
	bun build src/bin/worker.ts --compile --target="${bun_target}" \
		--outfile "${output_dir}/vgi-open-meteo"
)

echo "Building Rust worker"
if [[ -n "${VGI_RUST_TARGET:-}" ]]; then
	if ! command -v cargo-zigbuild >/dev/null 2>&1; then
		echo "VGI_RUST_TARGET requires cargo-zigbuild on PATH" >&2
		exit 1
	fi
	(
		cd "${rust_dir}"
		cargo zigbuild --locked --release --target "${VGI_RUST_TARGET}" -p vgi-example-worker
	)
	rust_binary="${rust_dir}/target/${VGI_RUST_TARGET}/release/vgi-example-worker"
else
	(
		cd "${rust_dir}"
		cargo build --locked --release -p vgi-example-worker
	)
	rust_binary="${rust_dir}/target/release/vgi-example-worker"
fi

# Use a multi-file archive for Rust so this lane proves safe tar.gz extraction,
# relative entrypoint selection, and execution rather than only raw BLOB copies.
mkdir -p "${scratch_dir}/rust-package/bin"
install -m 0755 "${rust_binary}" "${scratch_dir}/rust-package/bin/vgi-rust-worker"
printf '%s\n' "VGI Rust database-worker fixture" > "${scratch_dir}/rust-package/BUILD.txt"
tar -czf "${output_dir}/vgi-rust-worker.tar.gz" \
	-C "${scratch_dir}/rust-package" bin BUILD.txt

echo "Built database-worker artifacts in ${output_dir}"
ls -lh "${output_dir}/vgi-python-worker" \
	"${output_dir}/vgi-open-meteo" \
	"${output_dir}/vgi-rust-worker.tar.gz"
