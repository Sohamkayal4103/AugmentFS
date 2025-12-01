#!/bin/bash
set -e # Stop on first error

# --- Configuration ---
FS_BIN="./metadatafs"

# Use Absolute Paths so FUSE doesn't get lost when daemonizing
CURRENT_DIR=$(pwd)
BACKING="$CURRENT_DIR/backing_dir"
MOUNT="$CURRENT_DIR/mount_point"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo "=========================================="
echo "    METADATA FS: FULL WORKFLOW TEST"
echo "=========================================="

# Cleanup & Setup
echo -e "\n[Step 1] Cleaning up previous runs..."
# Use lazy unmount (-z) in case it's stuck
fusermount -u -z $MOUNT 2>/dev/null || true
rm -rf $BACKING $MOUNT
mkdir -p $BACKING $MOUNT

# Mount FS (Background)
echo -e "[Step 2] Mounting File System..."
# We enable append-only for "logs" directory
$FS_BIN $BACKING $MOUNT -o append_only_dirs=logs -f &
FS_PID=$!
sleep 2 # Wait for mount to initialize

if ! mount | grep -q "$MOUNT"; then
    echo -e "${RED}[ERROR] Mount failed!${NC}"
    exit 1
fi
echo -e "${GREEN}[OK] Mounted successfully.${NC}"

# Basic Write Test
echo -e "\n[Step 3] Test: Basic Write & Read"
echo "Hello World" > $MOUNT/hello.txt
CONTENT=$(cat $MOUNT/hello.txt)
if [ "$CONTENT" == "Hello World" ]; then
    echo -e "${GREEN}[PASS] Basic I/O works.${NC}"
else
    echo -e "${RED}[FAIL] Expected 'Hello World', got '$CONTENT'${NC}"
    exit 1
fi

# The Overwrite Test (Truncate Logic)
echo -e "\n[Step 4] Test: Overwrite (Truncate Logic)"
echo "Version 1" > $MOUNT/overwrite.txt
echo "Version 2" > $MOUNT/overwrite.txt
CONTENT=$(cat $MOUNT/overwrite.txt)
if [ "$CONTENT" == "Version 2" ]; then
    echo -e "${GREEN}[PASS] Overwrite works (Checksum updated correctly).${NC}"
else
    echo -e "${RED}[FAIL] Read failed or content mismatch.${NC}"
    exit 1
fi

# The Append Test (The Logic We Just Fixed)
echo -e "\n[Step 5] Test: Append Logic (The Bug Fix)"
printf "Start" > $MOUNT/append.txt
printf "End" >> $MOUNT/append.txt
CONTENT=$(cat $MOUNT/append.txt)
if [ "$CONTENT" == "StartEnd" ]; then
    echo -e "${GREEN}[PASS] Append works (Hash pre-loaded correctly).${NC}"
else
    echo -e "${RED}[FAIL] Append broken. Expected 'StartEnd', got '$CONTENT'${NC}"
    exit 1
fi

# Metadata Test (Xattrs)
echo -e "\n[Step 6] Test: Rich Metadata (Xattrs)"
touch $MOUNT/meta.txt
setfattr -n user.author -v "Soham" $MOUNT/meta.txt
AUTHOR=$(getfattr -n user.author --only-values $MOUNT/meta.txt)
if [ "$AUTHOR" == "Soham" ]; then
    echo -e "${GREEN}[PASS] Metadata storage works.${NC}"
else
    echo -e "${RED}[FAIL] Metadata retrieval failed.${NC}"
    exit 1
fi

# Integrity Test (Corruption Detection)
echo -e "\n[Step 7] Test: Data Integrity (Corruption)"
echo "CleanData" > $MOUNT/integrity.txt
cat $MOUNT/integrity.txt > /dev/null # Trigger verify once

# Manually corrupt the backing file
echo "DirtyData" > $BACKING/integrity.txt 

# Try to read through FUSE (Should fail)
if cat $MOUNT/integrity.txt > /dev/null 2>&1; then
    echo -e "${RED}[FAIL] System allowed reading corrupted file!${NC}"
    exit 1
else
    echo -e "${GREEN}[PASS] System correctly blocked corrupted file (EIO).${NC}"
fi

# WORM Security Test
echo -e "\n[Step 8] Test: WORM (Append-Only) Security"
mkdir $MOUNT/logs
echo "Log Entry 1" > $MOUNT/logs/sys.log

# Try to delete (Should fail)
if rm $MOUNT/logs/sys.log 2>/dev/null; then
    echo -e "${RED}[FAIL] WORM failed. File was deleted.${NC}"
    exit 1
else
    echo -e "${GREEN}[PASS] Blocked deletion in /logs.${NC}"
fi

# Try to overwrite (Should fail)
if echo "Hacked" > $MOUNT/logs/sys.log 2>/dev/null; then
    echo -e "${RED}[FAIL] WORM failed. File was overwritten.${NC}"
    exit 1
else
    echo -e "${GREEN}[PASS] Blocked overwrite in /logs.${NC}"
fi

# Cleanup
echo -e "\n[Step 9] Teardown"
kill $FS_PID
fusermount -u $MOUNT
echo -e "${GREEN}ALL TESTS PASSED SUCCESSFULLY!${NC}"