#!/bin/sh
set -eu
if [ "$#" -ne 3 ]; then
  echo 'Usage: sh reproduce.sh DISPOSABLE_BASELINE_CHECKOUT FIXTURE_DIRECTORY LOCAL_OUTPUT_DIRECTORY' >&2
  exit 2
fi
bundle_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
checkout_dir=$(CDPATH= cd -- "$1" && pwd)
fixture_dir=$(CDPATH= cd -- "$2" && pwd)
mkdir -p -- "$3"
output_dir=$(CDPATH= cd -- "$3" && pwd)
case "$output_dir/" in
  "$bundle_dir/"*|"$checkout_dir/"*) echo 'Output must be outside publication bundle and checkout.' >&2; exit 2 ;;
esac
expected_base=07f0272e7ba49a494064b6b74c6318b55514ae19
if [ "$(git -C "$checkout_dir" rev-parse HEAD)" != "$expected_base" ]; then
  echo "Expected disposable checkout at $expected_base." >&2
  exit 2
fi
git -C "$checkout_dir" diff --exit-code "$expected_base" -- packages examples/durable-bench/src examples/durable-bench/bench
bench_dir="$checkout_dir/examples/durable-bench"
helper_path="$checkout_dir/packages/effect-agent/src/durable/internal/history-prompt.ts"
if [ -e "$bench_dir/counting-hydration" ] || [ -e "$helper_path" ]; then
  echo 'Use a fresh disposable checkout; counting-hydration or helper already exists.' >&2
  exit 2
fi
for size in 50 250 1000 3500; do
  test -d "$fixture_dir/yielded-$size"
  test -f "$fixture_dir/yielded-$size.json"
done
cp -R "$bundle_dir/source/counting-hydration" "$bench_dir/counting-hydration"
candidate_root="$bench_dir/counting-hydration/candidate"
candidate_records="$candidate_root/packages/effect-agent/src/durable/Records.ts"
mkdir -p "$candidate_root/packages/effect-agent/src/durable/internal"
git -C "$checkout_dir" show "$expected_base:packages/effect-agent/src/durable/Records.ts" > "$candidate_records"
patch --silent -p1 -d "$candidate_root" < "$bundle_dir/candidate-0451aacb.patch"
cp "$candidate_root/packages/effect-agent/src/durable/internal/history-prompt.ts" "$helper_path"
python3 - "$candidate_records" "$helper_path" "$bundle_dir/provenance.json" "$bench_dir/package.json" <<'PY'
import hashlib,json,sys
from pathlib import Path
expected=json.loads(Path(sys.argv[3]).read_text())['candidateFiles']
for name,path in [('Records.ts',Path(sys.argv[1])),('history-prompt.ts',Path(sys.argv[2]))]:
    assert hashlib.sha256(path.read_bytes()).hexdigest()==expected[name]
p=Path(sys.argv[4]);value=json.loads(p.read_text());value['scripts']['count-hydration']='bun counting-hydration/run.ts';p.write_text(json.dumps(value,indent=2)+'\n')
PY
cd "$checkout_dir"
vp run --no-cache -F @yielded/agent-example-durable-bench count-hydration -- yielded baseline 50 250 1000 3500 --fixtures-dir "$fixture_dir" --candidate-records "$candidate_records" --out-dir "$output_dir/baseline"
vp run --no-cache -F @yielded/agent-example-durable-bench count-hydration -- yielded prompt-hydration 50 250 1000 3500 --fixtures-dir "$fixture_dir" --candidate-records "$candidate_records" --archive-dir "$output_dir/baseline" --out-dir "$output_dir/candidate"
vp run --no-cache -F @yielded/agent-example-durable-bench count-hydration -- yielded baseline 250 1000 --fixtures-dir "$fixture_dir" --candidate-records "$candidate_records" --archive-dir "$output_dir/baseline" --digest-scope on --out-dir "$output_dir/digest-baseline"
vp run --no-cache -F @yielded/agent-example-durable-bench count-hydration -- yielded prompt-hydration 250 1000 --fixtures-dir "$fixture_dir" --candidate-records "$candidate_records" --archive-dir "$output_dir/baseline" --digest-scope on --out-dir "$output_dir/digest-candidate"
