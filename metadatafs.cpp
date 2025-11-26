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
#include <sqlite3.h>
#include <vector> 
#include <unordered_map>
#include <sstream>
#include <cstdint>

static sqlite3* meta_db = nullptr;
static std::string backing_root;

static std::unordered_map<int, uint64_t> checksum_map; // fd -> running hash
// FNV-1a 64-bit constants
static const uint64_t FNV_OFFSET_BASIS = 1469598103934665603ULL;
static const uint64_t FNV_PRIME        = 1099511628211ULL;

// Update running FNV-1a hash with a buffer
static void update_fnv1a(uint64_t &hash, const char* buf, size_t size) {
    const unsigned char* p = reinterpret_cast<const unsigned char*>(buf);
    for (size_t i = 0; i < size; ++i) {
        hash ^= static_cast<uint64_t>(p[i]);
        hash *= FNV_PRIME;
    }
}

// Store/overwrite checksum(path) in the checksums table
static int store_checksum(const char* path, uint64_t hash) {
    if (!meta_db) return -EIO;

    // convert hash to hex string
    std::ostringstream oss;
    oss << std::hex << hash;
    std::string checksum = oss.str();

    const char* sql =
        "INSERT INTO checksums(path, checksum) "
        "VALUES(?, ?) "
        "ON CONFLICT(path) DO UPDATE SET checksum = excluded.checksum;";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(meta_db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        std::cerr << "store_checksum: prepare failed: "
                  << sqlite3_errmsg(meta_db) << std::endl;
        return -EIO;
    }

    sqlite3_bind_text(stmt, 1, path, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, checksum.c_str(), -1, SQLITE_TRANSIENT);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        std::cerr << "store_checksum: step failed for " << path << std::endl;
        return -EIO;
    }

    std::cout << "Stored checksum for " << path << ": " << checksum << std::endl;
    return 0;
}


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

static int fs_init_db() {
    std::string db_path = full_path("/.metadata.db");  // lives in backing_root
    std::cout << "Opening metadata DB at: " << db_path << std::endl;

    int rc = sqlite3_open(db_path.c_str(), &meta_db);
    if (rc != SQLITE_OK) {
        std::cerr << "sqlite3_open failed: " << sqlite3_errmsg(meta_db) << std::endl;
        return -1;
    }

    const char* sql =
        "CREATE TABLE IF NOT EXISTS metadata ("
        "  path TEXT NOT NULL,"
        "  key  TEXT NOT NULL,"
        "  value BLOB,"
        "  PRIMARY KEY(path, key)"
        ");"
        "CREATE TABLE IF NOT EXISTS checksums ("
        "  path TEXT PRIMARY KEY,"
        "  checksum TEXT"
        ");";

    char* errmsg = nullptr;
    rc = sqlite3_exec(meta_db, sql, nullptr, nullptr, &errmsg);
    if (rc != SQLITE_OK) {
        std::cerr << "sqlite3_exec failed: " << errmsg << std::endl;
        sqlite3_free(errmsg);
        return -1;
    }

    return 0;
}

static void* fs_init(struct fuse_conn_info* conn) {
    (void) conn;
    if (fs_init_db() != 0) {
        std::cerr << "Failed to init metadata DB" << std::endl;
    }
    return nullptr;
}

static void fs_destroy(void* private_data) {
    (void) private_data;
    if (meta_db) {
        std::cout << "Closing metadata DB" << std::endl;
        sqlite3_close(meta_db);
        meta_db = nullptr;
    }
}

static int fs_listxattr(const char* path, char* list, size_t size) {
    if (!meta_db) return -EIO;

    std::cout << "fs_listxattr: " << path << std::endl;

    const char* sql = "SELECT key FROM metadata WHERE path = ?;";
    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(meta_db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        std::cerr << "fs_listxattr: prepare failed: "
                  << sqlite3_errmsg(meta_db) << std::endl;
        return -EIO;
    }

    sqlite3_bind_text(stmt, 1, path, -1, SQLITE_TRANSIENT);

    // First pass: collect keys and compute required buffer size
    std::vector<std::string> keys;
    size_t required = 0;

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const unsigned char* key = sqlite3_column_text(stmt, 0);
        if (!key) continue;

        std::string k(reinterpret_cast<const char*>(key));
        required += k.size() + 1;   // +1 for '\0'
        keys.push_back(std::move(k));
    }

    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        std::cerr << "fs_listxattr: step failed\n";
        return -EIO;
    }

    // If caller only wants size
    if (list == nullptr || size == 0) {
        return (int)required;
    }

    if (size < required) {
        return -ERANGE;
    }

    // Second pass: pack keys separated by '\0'
    char* p = list;
    for (const auto& k : keys) {
        memcpy(p, k.c_str(), k.size());
        p += k.size();
        *p++ = '\0';
    }

    return (int)required;
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

    fi->fh = fd;    // store OS file descriptor in FUSE's file handle

    // Determine access mode
    int accmode = fi->flags & O_ACCMODE;
    bool is_writer = (accmode == O_WRONLY || accmode == O_RDWR);

    if (is_writer) {
        checksum_map[fd] = FNV_OFFSET_BASIS;  // initialize running hash
    }

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

    // Update checksum if this fd is tracked
    auto it = checksum_map.find(fd);
    if (it != checksum_map.end()) {
        update_fnv1a(it->second, buf, size);
    }

    ssize_t res = pwrite(fd, buf, size, offset);
    if (res == -1) {
        return -errno;
    }
    return res;
}

/*
 * fs_release: close the file when FUSE is done
 */
static int fs_release(const char* path, struct fuse_file_info* fi) {
    int fd = static_cast<int>(fi->fh);

    // Close underlying file
    int res = (close(fd) == -1) ? -errno : 0;

    // If we tracked a checksum for this fd, finalize & store it
    auto it = checksum_map.find(fd);
    if (it != checksum_map.end()) {
        uint64_t hash = it->second;
        checksum_map.erase(it);

        int rc = store_checksum(path, hash);
        if (rc != 0) {
            std::cerr << "fs_release: failed to store checksum for "
                      << path << std::endl;
        }
    }

    return res;
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

    int accmode = fi->flags & O_ACCMODE;
    bool is_writer = (accmode == O_WRONLY || accmode == O_RDWR);

    if (is_writer) {
        checksum_map[fd] = FNV_OFFSET_BASIS;
    }

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

    if (meta_db) {
        const char* sql1 = "DELETE FROM metadata WHERE path = ?;";
        const char* sql2 = "DELETE FROM checksums WHERE path = ?;";

        sqlite3_stmt* stmt = nullptr;

        if (sqlite3_prepare_v2(meta_db, sql1, -1, &stmt, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, path, -1, SQLITE_TRANSIENT);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }

        if (sqlite3_prepare_v2(meta_db, sql2, -1, &stmt, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, path, -1, SQLITE_TRANSIENT);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }
    }
    return 0;
}


static int fs_setxattr(const char* path, const char* name,
                       const char* value, size_t size, int flags) {
    (void) flags;

    if (!meta_db) return -EIO;

    std::cout << "fs_setxattr: " << path << " [" << name << "]" << std::endl;

    const char* sql =
        "INSERT INTO metadata(path, key, value) "
        "VALUES(?, ?, ?) "
        "ON CONFLICT(path, key) DO UPDATE SET value = excluded.value;";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(meta_db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return -EIO;
    }

    sqlite3_bind_text (stmt, 1, path, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text (stmt, 2, name, -1, SQLITE_TRANSIENT);
    sqlite3_bind_blob (stmt, 3, value, (int)size, SQLITE_TRANSIENT);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        return -EIO;
    }
    return 0;
}

static int fs_getxattr(const char* path, const char* name,
                       char* value, size_t size) {
    if (!meta_db) return -EIO;

    std::cout << "fs_getxattr: " << path << " [" << name << "]" << std::endl;

    const char* sql =
        "SELECT value FROM metadata WHERE path = ? AND key = ?;";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(meta_db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        return -EIO;
    }

    sqlite3_bind_text(stmt, 1, path, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, name, -1, SQLITE_TRANSIENT);

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        return -ENODATA;   // xattr not found
    }

    const void* blob = sqlite3_column_blob(stmt, 0);
    int blob_size    = sqlite3_column_bytes(stmt, 0);

    // First, caller may ask for size only (value == nullptr)
    if (value == nullptr || size == 0) {
        sqlite3_finalize(stmt);
        return blob_size;
    }

    if (size < (size_t)blob_size) {
        sqlite3_finalize(stmt);
        return -ERANGE;    // buffer too small
    }

    memcpy(value, blob, blob_size);
    sqlite3_finalize(stmt);
    return blob_size;      // number of bytes copied
}


// --- FUSE Operations Struct ---
static struct fuse_operations fs_ops;

void set_fs_ops() {
    fs_ops = {};  // Initialize to all zeros

    fs_ops.init     = fs_init;
    fs_ops.destroy  = fs_destroy;

    fs_ops.getattr  = fs_getattr;
    fs_ops.readdir  = fs_readdir;

    fs_ops.open     = fs_open;
    fs_ops.read     = fs_read;
    fs_ops.write    = fs_write;
    fs_ops.release  = fs_release;

    fs_ops.create   = fs_create;
    fs_ops.unlink   = fs_unlink;

    fs_ops.setxattr = fs_setxattr;
    fs_ops.getxattr = fs_getxattr;
    fs_ops.setxattr = fs_setxattr;
    fs_ops.listxattr = fs_listxattr;
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
