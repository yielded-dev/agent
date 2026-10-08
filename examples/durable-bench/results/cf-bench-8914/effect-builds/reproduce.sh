#!/bin/sh
set -eu
umask 077
evidence=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
task=$(mktemp -d /private/tmp/cf-bench-8914-effect-XXXXXXXX)
chmod 700 "$task"
git clone --filter=blob:none --no-checkout https://github.com/Effect-TS/effect.git "$task/upstream"
git -C "$task/upstream" fetch origin 01c6222ccf74390848595633ef23410cbfa6983b 13ace20e6c0501a0d01eb2b028672081c9780e42
base=$(git -C "$task/upstream" merge-base --all 01c6222ccf74390848595633ef23410cbfa6983b 13ace20e6c0501a0d01eb2b028672081c9780e42)
test "$base" = 757821fe99b7179f907d6d1a34a4e86de4173112
git -C "$task/upstream" worktree add --detach "$task/base" "$base"
git -C "$task/upstream" worktree add --detach "$task/head" 01c6222ccf74390848595633ef23410cbfa6983b
mkdir "$task/tooling" "$task/cache" "$task/tmp"
cp "$evidence/tooling.package.json" "$task/tooling/package.json"
cp "$evidence/tooling.bun.lock" "$task/tooling/bun.lock"
cd "$task/tooling"
TMPDIR="$task/tmp" BUN_INSTALL_CACHE_DIR="$task/cache" vp install --frozen-lockfile --ignore-scripts
vp node "$evidence/build.mjs" "$task"
printf '%s\n' "$task/build.json" "$task/packages/base/effect" "$task/packages/head/effect"
