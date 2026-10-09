#!/bin/sh
set -eu
if [ "$#" -ne 4 ]; then
  echo 'Usage: sh reproduce.sh DISPOSABLE_BASELINE_CHECKOUT FIXTURE_DIRECTORY ARCHIVE_DIRECTORY OUTPUT_DIRECTORY' >&2
  exit 2
fi
bundle_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
checkout_dir=$(CDPATH= cd -- "$1" && pwd)
fixture_dir=$(CDPATH= cd -- "$2" && pwd)
archive_dir=$(CDPATH= cd -- "$3" && pwd)
mkdir -p -- "$4"
output_dir=$(CDPATH= cd -- "$4" && pwd)
case "$output_dir/" in
  "$bundle_dir/"*|"$checkout_dir/"*|"$fixture_dir/"*) echo 'Output must be outside publication bundle, checkout, and fixture directory.' >&2; exit 2 ;;
esac
expected_base=07f0272e7ba49a494064b6b74c6318b55514ae19
expected_source=85f7a1de02672c8e023559cfcb9bd337b93cb7bf5ab01b3ff6f9b1462f970420
if [ "$(git -C "$checkout_dir" rev-parse HEAD)" != "$expected_base" ]; then
  echo "Expected disposable checkout at $expected_base." >&2
  exit 2
fi
git -C "$checkout_dir" diff --exit-code "$expected_base" -- packages examples/durable-bench/src
bench_dir="$checkout_dir/examples/durable-bench"
if [ -e "$bench_dir/counting-digest" ]; then
  echo 'Use a disposable checkout without an existing counting-digest directory.' >&2
  exit 2
fi
for size in 50 250 1000 3500; do
  test -d "$fixture_dir/yielded-$size"
  test -f "$fixture_dir/yielded-$size.json"
  test -f "$archive_dir/archive-$size.json"
done
python3 - "$bundle_dir" "$fixture_dir" "$archive_dir" <<'PY'
import hashlib,json,sys
from pathlib import Path
bundle,fixtures,archives=map(Path,sys.argv[1:])
provenance=json.loads((bundle/'provenance.json').read_text())
def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''):h.update(chunk)
    return h.hexdigest()
assert digest(bundle/'candidate/run-context.ts')==provenance['candidateSourceSha256']
for item in provenance['inputs']:
    size=item['size']
    assert digest(fixtures/f'yielded-{size}.json')==item['metadata']['sha256']
    assert digest(archives/f'archive-{size}.json')==item['archive']['sha256']
PY
cp -R "$bundle_dir/source/counting-digest" "$bench_dir/counting-digest"
cd "$bench_dir"
vp exec bun counting-digest/run.ts baseline 50 250 1000 3500 \
  --fixtures-dir "$fixture_dir" --archive-dir "$archive_dir" \
  --candidate-run-context "$bundle_dir/candidate/run-context.ts" --candidate-sha256 "$expected_source" \
  --out-dir "$output_dir/baseline"
vp exec bun counting-digest/run.ts digest-encoding 50 250 1000 3500 \
  --fixtures-dir "$fixture_dir" --archive-dir "$archive_dir" \
  --candidate-run-context "$bundle_dir/candidate/run-context.ts" --candidate-sha256 "$expected_source" \
  --out-dir "$output_dir/candidate"
vp exec python3 "$bundle_dir/aggregate.py" "$output_dir" \
  --candidate-revision 442c988c7485c6b088a10f56547daa70668fc0d6 \
  --expected-summary "$bundle_dir/summary.json" --out "$output_dir/summary.json"
