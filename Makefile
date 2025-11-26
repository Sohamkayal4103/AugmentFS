# Makefile for Metadata-Augmenting FUSE File System

# Compiler
CXX = g++

# Compiler flags
# -std=c++17: Use C++17 features
# -g: Add debug symbols
# `pkg-config`: Get correct FUSE flags
CXXFLAGS = -std=c++17 -g $(shell pkg-config fuse --cflags)

# Linker flags
LDFLAGS  = $(shell pkg-config fuse --libs) -lsqlite3

# Target executable name (the final program)
TARGET = metadatafs

# Source file (your code)
SOURCE = metadatafs.cpp

# Default rule: build the target
all: $(TARGET)

# The rule for building the target
$(TARGET): $(SOURCE)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SOURCE) $(LDFLAGS)

# Clean up build artifacts
clean:
	rm -f $(TARGET)

# Rule to run (for easy testing)
# We don't need a 'source_dir' for this simple task
run: all
	@echo "--- Setting up directories ---"
	@mkdir -p ./backing_dir
	@mkdir -p ./mount_point
	@echo "--- Running filesystem (in foreground) ---"
	@echo "Backing dir: ./backing_dir"
	@echo "Mount point: ./mount_point"
	@echo "--- Open another terminal to test ---"
	./$(TARGET) ./backing_dir ./mount_point -f

# Rule to unmount
unmount:
	fusermount -u ./mount_point

.PHONY: all clean run unmount