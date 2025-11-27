#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKING="$ROOT/backing_dir"
MOUNT="$ROOT/mount_point"
FS_BIN="$ROOT/metadatafs"

echo "== MetadataFS test harness =="
echo "Root:           $ROOT"
echo "Backing dir:    $BACKING"
echo "Mount point:    $MOUNT"
echo

# ---------- Helpers ----------

expect_fail() {
    local desc="$1"
    shift
    if "$@" 2>/dev/null; then
        echo "  [FAIL] $desc (command succeeded but should have failed)"
    else
        echo "  [OK]   $desc (command failed as expected)"
    fi
}

expect_success() {
    local desc="$1"
    shift
    if "$@"; then
        echo "  [OK]   $desc"
    else
        echo "  [FAIL] $desc (command failed unexpectedly)"
    fi
}

# ---------- Prep dirs ----------

echo "== Preparing directories =="
rm -rf "$BACKING" "$MOUNT"
mkdir -p "$BACKING" "$MOUNT"
echo "  backing_dir and mount_point reset."

# ---------- Start filesystem ----------

echo
echo "== Starting MetadataFS =="
"$FS_BIN" "$BACKING" "$MOUNT" -o append_only_dirs=logs,backups &
FS_PID=$!

# Give FUSE a moment to mount
sleep 1

# Verify it is mounted
if mount | grep -q "$MOUNT"; then
    echo "  Mounted on $MOUNT (PID=$FS_PID)"
else
    echo "  [ERROR] Filesystem did not mount on $MOUNT"
    kill "$FS_PID" 2>/dev/null || true
    exit 1
fi

echo

# ---------- Test 1: Basic pass-through ----------

echo "== Test 1: Basic passthrough (read/write) =="
cd "$MOUNT"

expect_success "create plain file" bash -c 'echo "hello world" > basic.txt'
expect_success "cat basic.txt" bash -c '[[ "$(cat basic.txt)" == "hello world" ]]'

echo

# ---------- Test 2: xattrs + SQLite mapping ----------

echo "== Test 2: Extended attributes via SQLite sidecar =="
expect_success "set xattr user.author"  setfattr -n user.author -v "Soham" basic.txt
expect_success "get xattr user.author"  bash -c 'getfattr -n user.author basic.txt | grep -q "Soham"'

echo "  Checking DB rows..."
cd "$BACKING"
sqlite3 .metadata.db 'SELECT path, key FROM metadata;' || true
cd "$MOUNT"
echo

# ---------- Test 3: Checksums on write + EIO on corruption ----------

echo "== Test 3: Checksums & corruption detection =="

# Create a file and read it through FUSE
expect_success "create testfile.txt" bash -c 'echo "this is clean data" > testfile.txt'
expect_success "read testfile.txt"   bash -c 'cat testfile.txt > /dev/null'

echo "  Current checksums in DB:"
cd "$BACKING"
sqlite3 .metadata.db 'SELECT path, checksum FROM checksums;' || true
cd "$MOUNT"

# Now corrupt backing file directly
echo "  Corrupting backing_dir/testfile.txt..."
cd "$BACKING"
printf "X" | dd of=testfile.txt bs=1 seek=0 conv=notrunc status=none
cd "$MOUNT"

expect_fail "corrupted file should give EIO" bash -c 'cat testfile.txt > /dev/null'

echo

# ---------- Test 4: Metadata + checksums removed on unlink ----------

echo "== Test 4: Unlink removes metadata + checksum =="

cd "$MOUNT"
echo "meta" > meta_test.txt
setfattr -n user.note -v "hello" meta_test.txt
cat meta_test.txt > /dev/null   # force checksum write

echo "  DB before unlink:"
cd "$BACKING"
sqlite3 .metadata.db 'SELECT path, key, checksum FROM metadata LEFT JOIN checksums USING(path);'
cd "$MOUNT"

expect_success "unlink meta_test.txt" rm meta_test.txt

echo "  DB after unlink:"
cd "$BACKING"
sqlite3 .metadata.db 'SELECT path, key, checksum FROM metadata LEFT JOIN checksums USING(path);'
cd "$MOUNT"

echo

# ---------- Test 5: Rename updates DB paths ----------

echo "== Test 5: Rename updates metadata + checksum paths =="

cd "$MOUNT"
echo "hello" > rename_me.txt
setfattr -n user.note -v "before" rename_me.txt
cat rename_me.txt > /dev/null   # compute checksum

echo "  DB before rename:"
cd "$BACKING"
sqlite3 .metadata.db 'SELECT path, key, checksum FROM metadata LEFT JOIN checksums USING(path) WHERE path LIKE "/rename_me%";'
cd "$MOUNT"

expect_success "rename within FS" mv rename_me.txt renamed.txt

echo "  DB after rename:"
cd "$BACKING"
sqlite3 .metadata.db 'SELECT path, key, checksum FROM metadata LEFT JOIN checksums USING(path) WHERE path LIKE "/renamed%";'
cd "$MOUNT"

echo

# ---------- Test 6: Append-only WORM mode ----------

echo "== Test 6: Append-only WORM behavior (logs,backups) =="

cd "$MOUNT"

expect_success "create logs directory" mkdir -p logs
expect_success "create file inside append-only dir" bash -c 'echo "data" > logs/a.txt'

expect_fail "rm logs/a.txt (unlink blocked)" rm logs/a.txt
expect_fail "truncate logs/a.txt via truncate(2)" truncate -s 0 logs/a.txt
expect_fail "truncate logs/a.txt via O_TRUNC" bash -c ': > logs/a.txt'

expect_fail "rename out of append-only dir" mv logs/a.txt ../a_outside.txt

echo "x" > normal.txt
expect_fail "rename into append-only dir" mv normal.txt logs/inside.txt

echo

# ---------- Cleanup: unmount and stop fs ----------

echo "== Cleanup =="
cd "$ROOT"

if mount | grep -q "$MOUNT"; then
    echo "  Unmounting $MOUNT..."
    fusermount -u "$MOUNT"
fi

if kill "$FS_PID" 2>/dev/null; then
    echo "  Killed MetadataFS (PID=$FS_PID)"
fi

echo
echo "== All tests completed. Check [OK]/[FAIL] markers above. =="
