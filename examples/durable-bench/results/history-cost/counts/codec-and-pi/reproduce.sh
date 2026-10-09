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
  "$bundle_dir/"*|"$checkout_dir/"*) echo 'Output must be outside the publication bundle and checkout.' >&2; exit 2 ;;
esac
expected_base=07f0272e7ba49a494064b6b74c6318b55514ae19
if [ "$(git -C "$checkout_dir" rev-parse HEAD)" != "$expected_base" ]; then
  echo "Expected baseline $expected_base in a disposable checkout." >&2
  exit 2
fi
bench_dir="$checkout_dir/examples/durable-bench"
if [ -e "$bench_dir/counting" ]; then
  echo 'Use a fresh disposable checkout: counting/ already exists.' >&2
  exit 2
fi
for target in yielded pi; do
  for size in 50 250 1000 3500; do
    test -d "$fixture_dir/$target-$size"
    test -f "$fixture_dir/$target-$size.json"
  done
done
cp -R "$bundle_dir/source/counting" "$bench_dir/counting"
git -C "$checkout_dir" show "$expected_base:packages/effect-agent/src/durable/Records.ts" > "$bench_dir/counting/candidate-Records.ts"
patch --silent "$bench_dir/counting/candidate-Records.ts" < "$bundle_dir/candidate-40485f33.patch"
python3 - "$bench_dir/package.json" <<'PY'
import json,sys
from pathlib import Path
p=Path(sys.argv[1]); value=json.loads(p.read_text())
value['scripts']['count']='bun counting/run.ts'
p.write_text(json.dumps(value,indent=2)+'\n')
PY
cd "$checkout_dir"
vp run --no-cache -F @yielded/agent-example-durable-bench count -- yielded baseline 50 250 1000 3500 --fixtures-dir "$fixture_dir" --out-dir "$output_dir"
vp run --no-cache -F @yielded/agent-example-durable-bench count -- yielded candidate 50 250 1000 3500 --fixtures-dir "$fixture_dir" --out-dir "$output_dir"
vp run --no-cache -F @yielded/agent-example-durable-bench count -- pi baseline 50 250 1000 3500 --fixtures-dir "$fixture_dir" --out-dir "$output_dir"
