# Compiler
CXX = g++

# Compiler flags
# -std=c++17: Use C++17 features
# -g: Add debug symbols
# `pkg-config`: Get correct FUSE flags
CXXFLAGS = -std=c++17 -g $(shell pkg-config fuse --cflags)

# Linker flags
LDFLAGS  = $(shell pkg-config fuse --libs) -lsqlite3

# Targets
TARGET_GOOD = metadatafs
TARGET_BAD  = metadatafs_bad

# Source files
SOURCE_GOOD = metadatafs.cpp
SOURCE_BAD  = metadatafs_bad.cpp

TARGET_BLOCK = blockfs
SOURCE_BLOCK = blockfs.cpp

# Default rule: build both targets
all: $(TARGET_GOOD) $(TARGET_BAD) $(TARGET_BLOCK)

# Rule for optimized FS
$(TARGET_GOOD): $(SOURCE_GOOD)
	$(CXX) $(CXXFLAGS) -o $(TARGET_GOOD) $(SOURCE_GOOD) $(LDFLAGS)

# Rule for bad FS
$(TARGET_BAD): $(SOURCE_BAD)
	$(CXX) $(CXXFLAGS) -o $(TARGET_BAD) $(SOURCE_BAD) $(LDFLAGS)

# Rule for block FS
$(TARGET_BLOCK): $(SOURCE_BLOCK)
	$(CXX) $(CXXFLAGS) -o $(TARGET_BLOCK) $(SOURCE_BLOCK) $(LDFLAGS)

# Clean up build artifacts
clean:
	rm -f $(TARGET_GOOD) $(TARGET_BAD) $(TARGET_BLOCK)
# Rule to run Optimized FS (for manual testing)
run: $(TARGET_GOOD)
	@echo "--- Setting up directories ---"
	@mkdir -p ./backing_dir
	@mkdir -p ./mount_point
	@echo "--- Running OPTIMIZED filesystem ---"
	./$(TARGET_GOOD) ./backing_dir ./mount_point -f

# Rule to run Block FS
run_block: $(TARGET_BLOCK)
	@echo "--- Cleaning directories for BlockFS ---"
	@fusermount -u ./mount_point 2>/dev/null || true
	@rm -rf ./backing_dir ./mount_point
	@mkdir -p ./backing_dir
	@mkdir -p ./mount_point
	@echo "--- Running BLOCK-LEVEL filesystem ---"
	./$(TARGET_BLOCK) ./backing_dir ./mount_point -f

# Rule to unmount
unmount:
	fusermount -u ./mount_point || true

.PHONY: all clean run unmount