#define FUSE_USE_VERSION 29
#define _FILE_OFFSET_BITS 64

#include <fuse.h>
#include <string.h>
#include <errno.h>
#include <iostream>

/*
 * fs_getattr: "Get Attributes"
 * Linux calls this for 'ls -l', 'cd', and almost every command.
 * 'path': The path being asked about (e.g., "/" or "/hello.txt")
 * 'st': A struct needed to fill in with information (file size, permissions, etc.)
 */
static int fs_getattr(const char* path, struct stat* st) {
    // Clear the struct to zeros
    memset(st, 0, sizeof(struct stat));
    std::cout << "fs_getattr called for path: " << path << std::endl;
    if (strcmp(path, "/") == 0) {
        st->st_mode = S_IFDIR | 0755;
        // It has 2 "links" (standard for directories: '.' and '..')
        st->st_nlink = 2;
        return 0;
    }
    return -ENOENT; // "Error: No Such Entity"
}

/*
 * fs_readdir: "Read Directory"
 * This function is called when you run 'ls' on a directory.
 * Its job is to tell FUSE what files are *inside* that directory.
 *
 * 'path': The directory being read (e.g., "/")
 * 'buf': A buffer to put the file names into
 * 'filler': A special function FUSE gives us to add names to the buffer
 */
static int fs_readdir(const char* path, void* buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info* fi) {
    (void) offset;
    (void) fi;

    // Only read the root directory "/"
    if (strcmp(path, "/") != 0) {
        return -ENOENT;
    }

    // All directories must at least contain:
    filler(buf, ".", NULL, 0); 
    filler(buf, "..", NULL, 0);

    return 0;
}

// --- FUSE Operations Struct ---
/*
 * This struct is the "table of contents." 
 */
static struct fuse_operations fs_ops;

void set_fs_ops() {
    fs_ops = {}; // Initialize to all zeros
    fs_ops.getattr  = fs_getattr;
    fs_ops.readdir  = fs_readdir;
}

int main(int argc, char* argv[]) {
    // Set up our operations "table of contents"
    set_fs_ops();

    std::cout << "=========================================" << std::endl;
    std::cout << "MetadataFS (Task 1) Mounting..." << std::endl;
    std::cout << "=========================================" << std::endl;

    // Start FUSE
    // This 'fuse_main' function takes over our program
    // and runs forever until someone press Ctrl+C.
    int fuse_ret = fuse_main(argc, argv, &fs_ops, NULL);

    std::cout << "Unmounted filesystem." << std::endl;

    return fuse_ret;
}