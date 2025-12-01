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

# Default rule: build both targets
all: $(TARGET_GOOD) $(TARGET_BAD)

# Rule for optimized FS
$(TARGET_GOOD): $(SOURCE_GOOD)
	$(CXX) $(CXXFLAGS) -o $(TARGET_GOOD) $(SOURCE_GOOD) $(LDFLAGS)

# Rule for bad FS
$(TARGET_BAD): $(SOURCE_BAD)
	$(CXX) $(CXXFLAGS) -o $(TARGET_BAD) $(SOURCE_BAD) $(LDFLAGS)

# Clean up build artifacts
clean:
	rm -f $(TARGET_GOOD) $(TARGET_BAD)

# Rule to run Optimized FS (for manual testing)
run: $(TARGET_GOOD)
	@echo "--- Setting up directories ---"
	@mkdir -p ./backing_dir
	@mkdir -p ./mount_point
	@echo "--- Running OPTIMIZED filesystem ---"
	./$(TARGET_GOOD) ./backing_dir ./mount_point -f

# Rule to unmount
unmount:
	fusermount -u ./mount_point || true

.PHONY: all clean run unmount