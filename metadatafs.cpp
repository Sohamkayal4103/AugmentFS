#define FUSE_USE_VERSION 29
#define _FILE_OFFSET_BITS 64

#include <fuse.h>
#include <string.h>
#include <errno.h>
#include <iostream>
#include <string>

#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>

static std::string backing_root;

// Join backing_root + FUSE path (e.g. "/foo.txt")
static std::string full_path(const char* path) {
    std::string result = backing_root;

    // Ensure no trailing '/' on backing_root
    if (!result.empty() && result.back() == '/') {
        result.pop_back();
    }

    result += path;  // FUSE paths always start with '/'
    return result;
}

/*
 * fs_getattr: pass-through "stat"
 */
static int fs_getattr(const char* path, struct stat* st) {
    memset(st, 0, sizeof(struct stat));

    std::string real = full_path(path);
    std::cout << "fs_getattr: " << path << " -> " << real << std::endl;

    if (lstat(real.c_str(), st) == -1) {
        return -errno;   // map OS errno to FUSE error
    }
    return 0;
}

/*
 * fs_readdir: pass-through "ls"
 */
static int fs_readdir(const char* path, void* buf, fuse_fill_dir_t filler,
                      off_t offset, struct fuse_file_info* fi) {
    (void) offset;
    (void) fi;

    std::string real = full_path(path);
    std::cout << "fs_readdir: " << path << " -> " << real << std::endl;

    DIR* dp = opendir(real.c_str());
    if (dp == nullptr) {
        return -errno;
    }

    // Every directory must have "." and ".."
    filler(buf, ".",  nullptr, 0);
    filler(buf, "..", nullptr, 0);

    struct dirent* de;
    while ((de = readdir(dp)) != nullptr) {
        const char* name = de->d_name;

        if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
            continue;
        }

        if (filler(buf, name, nullptr, 0) != 0) {
            closedir(dp);
            return -ENOMEM;
        }
    }

    closedir(dp);
    return 0;
}

/*
 * fs_open: open a file in the backing directory
 */
static int fs_open(const char* path, struct fuse_file_info* fi) {
    std::string real = full_path(path);
    std::cout << "fs_open: " << path << " -> " << real << std::endl;

    int fd = open(real.c_str(), fi->flags);
    if (fd == -1) {
        return -errno;
    }

    fi->fh = fd;  // store OS file descriptor in FUSE's file handle
    return 0;
}

/*
 * fs_read: read from an already-open file
 */
static int fs_read(const char* path, char* buf, size_t size,
                   off_t offset, struct fuse_file_info* fi) {
    (void) path;  // unused because we already have fd

    int fd = static_cast<int>(fi->fh);
    ssize_t res = pread(fd, buf, size, offset);
    if (res == -1) {
        return -errno;
    }
    return res;   // number of bytes read
}

/*
 * fs_write: write to an already-open file
 */
static int fs_write(const char* path, const char* buf, size_t size,
                    off_t offset, struct fuse_file_info* fi) {
    (void) path;

    int fd = static_cast<int>(fi->fh);
    ssize_t res = pwrite(fd, buf, size, offset);
    if (res == -1) {
        return -errno;
    }
    return res;   // number of bytes written
}

/*
 * fs_release: close the file when FUSE is done
 */
static int fs_release(const char* path, struct fuse_file_info* fi) {
    (void) path;

    int fd = static_cast<int>(fi->fh);
    if (close(fd) == -1) {
        return -errno;
    }
    return 0;
}

/*
 * fs_create: create a new file
 */
static int fs_create(const char* path, mode_t mode, struct fuse_file_info* fi) {
    std::string real = full_path(path);
    std::cout << "fs_create: " << path << " -> " << real << std::endl;

    int fd = open(real.c_str(), fi->flags, mode);
    if (fd == -1) {
        return -errno;
    }

    fi->fh = fd;
    return 0;
}

/*
 * fs_unlink: delete a file
 */
static int fs_unlink(const char* path) {
    std::string real = full_path(path);
    std::cout << "fs_unlink: " << path << " -> " << real << std::endl;

    if (unlink(real.c_str()) == -1) {
        return -errno;
    }
    return 0;
}


// --- FUSE Operations Struct ---
static struct fuse_operations fs_ops;

void set_fs_ops() {
    fs_ops = {}; // Initialize to all zeros

    fs_ops.getattr  = fs_getattr;
    fs_ops.readdir  = fs_readdir;

    fs_ops.open     = fs_open;
    fs_ops.read     = fs_read;
    fs_ops.write    = fs_write;
    fs_ops.release  = fs_release;

    fs_ops.create   = fs_create;
    fs_ops.unlink   = fs_unlink;
}


int main(int argc, char* argv[]) {
    // We expect: ./metadatafs <backing_dir> <mount_point> [FUSE options...]
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0]
                  << " <backing_dir> <mount_point> [FUSE options...]\n";
        return 1;
    }

    backing_root = argv[1];

    // Shift args left so FUSE sees: prog <mount_point> [options...]
    for (int i = 1; i < argc - 1; ++i) {
        argv[i] = argv[i + 1];
    }
    argc--;

    set_fs_ops();

    std::cout << "=========================================\n";
    std::cout << "MetadataFS (Task 1) Mounting...\n";
    std::cout << "Backing directory: " << backing_root << "\n";
    std::cout << "=========================================\n";

    int fuse_ret = fuse_main(argc, argv, &fs_ops, NULL);

    std::cout << "Unmounted filesystem.\n";
    return fuse_ret;
}
