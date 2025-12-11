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
#include <unordered_set>
#include <sstream>
#include <cstdint>
#include <cstring>

static sqlite3* meta_db = nullptr;
static std::string backing_root;

static std::unordered_map<int, uint64_t> checksum_map; // fd -> running hash
// FNV-1a 64-bit constants
static const uint64_t FNV_OFFSET_BASIS = 1469598103934665603ULL;
static const uint64_t FNV_PRIME        = 1099511628211ULL;

// For read-time verification
static std::unordered_set<int> verified_ok_fds;    // fds whose checksum matched
static std::unordered_set<int> verified_bad_fds;   // fds with checksum mismatch
static std::vector<std::string> append_only_dirs;  // e.g., "/logs", "/backups"
static std::unordered_multimap<std::string, int> open_path_to_fd;

static bool is_append_only_path(const char* path);

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

// Compute FNV-1a checksum of the entire file at real_path (used for reads)
static std::string compute_checksum_for_file(const std::string& real_path) {
    int fd = open(real_path.c_str(), O_RDONLY);
    if (fd == -1) {
        std::cerr << "compute_checksum_for_file: failed to open "
                  << real_path << std::endl;
        return "";
    }

    uint64_t hash = FNV_OFFSET_BASIS;
    char buf[4096];
    ssize_t n;

    while ((n = read(fd, buf, sizeof(buf))) > 0) {
        update_fnv1a(hash, buf, (size_t)n);
    }

    if (n == -1) {
        std::cerr << "compute_checksum_for_file: read error on "
                  << real_path << std::endl;
        close(fd);
        return "";
    }

    close(fd);

    std::ostringstream oss;
    oss << std::hex << hash;
    return oss.str();
}

// Verify checksum for (path, fd) once. Cache result in verified_*_fds.
static bool verify_fd_checksum(const char* path, int fd) {
    // If we've already verified or rejected this fd, just return cached result.
    if (verified_ok_fds.count(fd)) {
        return true;
    }
    if (verified_bad_fds.count(fd)) {
        return false;
    }

    if (!meta_db) {
        // No DB means no integrity info; allow read.
        verified_ok_fds.insert(fd);
        return true;
    }

    // 1. Look up stored checksum from DB
    const char* sql = "SELECT checksum FROM checksums WHERE path = ?;";
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(meta_db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        std::cerr << "verify_fd_checksum: prepare failed: "
                  << sqlite3_errmsg(meta_db) << std::endl;
        verified_ok_fds.insert(fd);  // fail-open
        return true;
    }

    sqlite3_bind_text(stmt, 1, path, -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(stmt);

    if (rc != SQLITE_ROW) {
        // No stored checksum for this path -> treat as unprotected, allow read.
        sqlite3_finalize(stmt);
        verified_ok_fds.insert(fd);
        return true;
    }

    const unsigned char* checksum_text = sqlite3_column_text(stmt, 0);
    std::string stored_checksum =
        checksum_text ? reinterpret_cast<const char*>(checksum_text) : "";

    sqlite3_finalize(stmt);

    if (stored_checksum.empty()) {
        // Weird, but fail-open
        verified_ok_fds.insert(fd);
        return true;
    }

    // 2. Compute current checksum from the backing file
    std::string real = full_path(path);
    std::string current = compute_checksum_for_file(real);

    if (current.empty()) {
        // Could not compute; conservative choice: treat as bad
        std::cerr << "verify_fd_checksum: empty current checksum for "
                  << path << std::endl;
        verified_bad_fds.insert(fd);
        return false;
    }

    if (current == stored_checksum) {
        std::cout << "verify_fd_checksum: OK for " << path
                  << " (checksum " << current << ")\n";
        verified_ok_fds.insert(fd);
        return true;
    } else {
        std::cerr << "verify_fd_checksum: MISMATCH for " << path
                  << " stored=" << stored_checksum
                  << " current=" << current << std::endl;
        verified_bad_fds.insert(fd);
        return false;
    }
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


static uint64_t compute_hash_uint64(const std::string& real_path) {
    int fd = open(real_path.c_str(), O_RDONLY);
    if (fd == -1) {
        // If file can't be opened, return default empty hash
        return FNV_OFFSET_BASIS;
    }

    uint64_t hash = FNV_OFFSET_BASIS;
    char buf[4096];
    ssize_t n;

    while ((n = read(fd, buf, sizeof(buf))) > 0) {
        update_fnv1a(hash, buf, (size_t)n);
    }

    close(fd);
    return hash;
}

/*
 * fs_open: open a file in the backing directory
 */
static int fs_open(const char* path, struct fuse_file_info* fi) {
    if (is_append_only_path(path) && (fi->flags & O_TRUNC)) {
        std::cout << "fs_open: DENY O_TRUNC (append-only) " << path << std::endl;
        return -EPERM;
    }

    std::string real = full_path(path);
    std::cout << "fs_open: " << path << " -> " << real << std::endl;

    // 1. Open the real file
    int fd = open(real.c_str(), fi->flags);
    if (fd == -1) return -errno;

    fi->fh = fd; 
    verified_ok_fds.erase(fd);
    verified_bad_fds.erase(fd);

    int accmode = fi->flags & O_ACCMODE;
    bool is_writer = (accmode == O_WRONLY || accmode == O_RDWR);

    if (is_writer) {
        if (fi->flags & O_TRUNC) {
            // Overwrite: Old data irrelevant. Start fresh.
            checksum_map[fd] = FNV_OFFSET_BASIS;
        } else {
            // STRICT APPEND LOGIC
            
            // 1. Compute hash of what is currently on disk
            uint64_t disk_hash_val = compute_hash_uint64(real);
            
            // 2. Fetch what the DB thinks the hash should be
            if (meta_db) {
                const char* sql = "SELECT checksum FROM checksums WHERE path = ?;";
                sqlite3_stmt* stmt = nullptr;
                if (sqlite3_prepare_v2(meta_db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
                    sqlite3_bind_text(stmt, 1, path, -1, SQLITE_TRANSIENT);
                    
                    int rc = sqlite3_step(stmt);
                    if (rc == SQLITE_ROW) {
                        const unsigned char* txt = sqlite3_column_text(stmt, 0);
                        std::string db_hash = txt ? reinterpret_cast<const char*>(txt) : "";
                        
                        // Convert our disk calculation to string for comparison
                        std::ostringstream oss;
                        oss << std::hex << disk_hash_val;
                        std::string disk_hash_str = oss.str();

                        // 3. STRICT CHECK
                        if (!db_hash.empty() && db_hash != disk_hash_str) {
                            std::cerr << "fs_open: STRICT INTEGRITY CHECK FAILED on Append!" << std::endl;
                            std::cerr << "   DB Says:   " << db_hash << std::endl;
                            std::cerr << "   Disk Says: " << disk_hash_str << std::endl;
                            
                            sqlite3_finalize(stmt);
                            close(fd); // Close the file we just opened
                            return -EIO; // BLOCK THE OPEN
                        }
                    }
                    sqlite3_finalize(stmt);
                }
            }

            // Check passed (or DB was empty). Load the hash and proceed.
            checksum_map[fd] = disk_hash_val;
            std::cout << "fs_open: Integrity verified. Pre-loaded hash for append." << std::endl;
        }
    }

    open_path_to_fd.insert({std::string(path), fd});
    return 0;
}

/*
 * fs_read: read from an already-open file
 */
static int fs_read(const char* path, char* buf, size_t size,
                   off_t offset, struct fuse_file_info* fi) {
    int fd = static_cast<int>(fi->fh);

    // If this fd is NOT being tracked as a writer, enforce checksum verification
    if (checksum_map.find(fd) == checksum_map.end()) {
        if (!verify_fd_checksum(path, fd)) {
            return -EIO;
        }
    }

    ssize_t res = pread(fd, buf, size, offset);
    if (res == -1) {
        return -errno;
    }
    return res;
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

    auto range = open_path_to_fd.equal_range(path);
    for (auto it = range.first; it != range.second; ) {
        if (it->second == fd) {
            it = open_path_to_fd.erase(it);
        } else {
            ++it;
        }
    }

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

    verified_ok_fds.erase(fd);
    verified_bad_fds.erase(fd);

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

// Return true if path is inside any append-only directory
static bool is_append_only_path(const char* path) {
    if (append_only_dirs.empty()) return false;

    std::string p(path);  // e.g., "/logs/foo.txt"

    for (const auto& dir : append_only_dirs) {
        // dir is like "/logs"
        if (p == dir) return true;
        if (p.size() > dir.size() &&
            p.compare(0, dir.size(), dir) == 0 &&
            p[dir.size()] == '/') {
            return true;
        }
    }
    return false;
}

static int fs_utimens(const char* path, const struct timespec tv[2]) {
    std::string real = full_path(path);
    // utimensat with 0 flags updates the time on the real underlying file
    if (utimensat(0, real.c_str(), tv, 0) == -1) {
        return -errno;
    }
    return 0;
}

/*
 * fs_unlink: delete a file
 */
static int fs_unlink(const char* path) {
    if (is_append_only_path(path)) {
        std::cout << "fs_unlink: DENY (append-only) " << path << std::endl;
        return -EPERM;
    }

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

static int fs_truncate(const char* path, off_t size) {
    if (is_append_only_path(path)) {
        std::cout << "fs_truncate: DENY (append-only) " << path << std::endl;
        return -EPERM;
    }

    std::string real = full_path(path);
    std::cout << "fs_truncate: " << path << " -> " << real
              << " size=" << size << std::endl;

    if (truncate(real.c_str(), size) == -1) {
        return -errno;
    }

    // 1. Calculate the NEW hash of the file on disk (handles size=0 or size=N)
    uint64_t new_hash = compute_hash_uint64(real);
    
    // 2. Update the Database
    store_checksum(path, new_hash);

    // 3. Update any OPEN file descriptors
    auto range = open_path_to_fd.equal_range(path);
    for (auto it = range.first; it != range.second; ++it) {
        int fd = it->second;
        
        // CRITICAL CHECK: Only update if this FD is actually a WRITER.
        // If we don't check this, we might accidentally add a read-only FD 
        // to the checksum_map, breaking future reads.
        if (checksum_map.count(fd)) {
            checksum_map[fd] = new_hash; 
            std::cout << "fs_truncate: Updated running hash for FD " << fd << std::endl;
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

static int fs_rename(const char* from, const char* to) {
    // If either source or destination lives under an append-only dir, block.
    if (is_append_only_path(from) || is_append_only_path(to)) {
        std::cout << "fs_rename: DENY (append-only) from=" << from
                  << " to=" << to << std::endl;
        return -EPERM;
    }

    std::string real_from = full_path(from);
    std::string real_to   = full_path(to);

    std::cout << "fs_rename: " << from << " -> " << to
              << "  (" << real_from << " -> " << real_to << ")\n";

    if (::rename(real_from.c_str(), real_to.c_str()) == -1) {
        return -errno;
    }

    if (meta_db) {
        // Update metadata table
        const char* sql_meta =
            "UPDATE metadata SET path = ? WHERE path = ?;";
        sqlite3_stmt* stmt = nullptr;

        if (sqlite3_prepare_v2(meta_db, sql_meta, -1, &stmt, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, to,   -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 2, from, -1, SQLITE_TRANSIENT);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }

        // Update checksums table
        const char* sql_chk =
            "UPDATE checksums SET path = ? WHERE path = ?;";

        if (sqlite3_prepare_v2(meta_db, sql_chk, -1, &stmt, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, to,   -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 2, from, -1, SQLITE_TRANSIENT);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }
    }

    return 0;
}

static void add_append_only_dirs_from_csv(const char* csv) {
    if (!csv) return;
    std::stringstream ss(csv);
    std::string item;

    while (std::getline(ss, item, ',')) {
        if (item.empty()) continue;
        // no fancy trimming; assume clean names like "logs" or "backups"
        if (item[0] != '/') {
            item = "/" + item;  // make it absolute from FS root
        }
        append_only_dirs.push_back(item);
        std::cout << "Append-only dir configured: " << item << std::endl;
    }
}

// Scan argv (starting at index 2: after backing_root) for append_only_dirs=...
// and remove that option from argv so FUSE doesn't see it.
static void parse_append_only_option(int& argc, char* argv[]) {
    // We assume:
    // argv[0] = prog
    // argv[1] = backing_root
    // argv[2] = mount_point or FUSE arg
    int i = 2;
    while (i < argc) {
        // Case 1: "-o", "append_only_dirs=logs,backups"
        if (strcmp(argv[i], "-o") == 0 && i + 1 < argc) {
            const char* opt = argv[i + 1];
            const char* key = "append_only_dirs=";
            const char* pos = strstr(opt, key);
            if (pos == opt) {
                const char* csv = opt + strlen(key);
                add_append_only_dirs_from_csv(csv);

                // Remove opt (argv[i+1])
                for (int j = i + 1; j < argc - 1; ++j) {
                    argv[j] = argv[j + 1];
                }
                --argc;

                // Remove "-o" (argv[i])
                for (int j = i; j < argc - 1; ++j) {
                    argv[j] = argv[j + 1];
                }
                --argc;

                // Don't advance i; new arg now sits at position i
                continue;
            }
        }

        // Case 2: "-oappend_only_dirs=logs,backups"
        const char* key2 = "-oappend_only_dirs=";
        if (strncmp(argv[i], key2, strlen(key2)) == 0) {
            const char* csv = argv[i] + strlen(key2);
            add_append_only_dirs_from_csv(csv);

            // Remove this single argv[i]
            for (int j = i; j < argc - 1; ++j) {
                argv[j] = argv[j + 1];
            }
            --argc;
            continue;
        }

        ++i;
    }
}

/*
 * fs_mkdir: create a directory in the backing store
 */
static int fs_mkdir(const char* path, mode_t mode) {
    std::string real = full_path(path);
    std::cout << "fs_mkdir: " << path << " -> " << real << std::endl;

    if (mkdir(real.c_str(), mode) == -1) {
        return -errno;
    }
    return 0;
}

/*
 * fs_rmdir: remove a directory
 */
static int fs_rmdir(const char* path) {
    std::string real = full_path(path);
    std::cout << "fs_rmdir: " << path << " -> " << real << std::endl;

    if (rmdir(real.c_str()) == -1) {
        return -errno;
    }
    return 0;
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
    fs_ops.listxattr = fs_listxattr;

    fs_ops.rename = fs_rename;
    fs_ops.truncate = fs_truncate;

    fs_ops.mkdir    = fs_mkdir;
    fs_ops.rmdir    = fs_rmdir;

    fs_ops.utimens  = fs_utimens;
}

int main(int argc, char* argv[]) {
    // We expect: ./metadatafs <backing_dir> <mount_point> [FUSE options...]
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0]
                  << " <backing_dir> <mount_point> [FUSE options...]\n";
        return 1;
    }

    backing_root = argv[1];

    // Parse and strip our custom append-only option
    parse_append_only_option(argc, argv);
    // After this, backing_root is still argv[1], but the -o append_only_dirs=... args are gone.

    // Shift args left so FUSE sees: prog <mount_point> [options...]
    for (int i = 1; i < argc - 1; ++i) {
        argv[i] = argv[i + 1];
    }
    --argc;

    set_fs_ops();

    std::cout << "=========================================\n";
    std::cout << "MetadataFS (Task 1) Mounting...\n";
    std::cout << "Backing directory: " << backing_root << "\n";
    if (!append_only_dirs.empty()) {
        std::cout << "Append-only dirs enabled.\n";
    }
    std::cout << "=========================================\n";

    int fuse_ret = fuse_main(argc, argv, &fs_ops, NULL);

    std::cout << "Unmounted filesystem.\n";
    return fuse_ret;
}

