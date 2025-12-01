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
#include <iomanip>

static sqlite3* meta_db = nullptr;
static std::string backing_root;

static std::unordered_map<int, uint64_t> checksum_map;
static const uint64_t FNV_OFFSET_BASIS = 1469598103934665603ULL;
static const uint64_t FNV_PRIME        = 1099511628211ULL;

static std::unordered_set<int> verified_ok_fds;
static std::unordered_set<int> verified_bad_fds;
static std::vector<std::string> append_only_dirs;

static bool is_append_only_path(const char* path);

// Helper hash function
static void update_fnv1a(uint64_t &hash, const char* buf, size_t size) {
    const unsigned char* p = reinterpret_cast<const unsigned char*>(buf);
    for (size_t i = 0; i < size; ++i) {
        hash ^= static_cast<uint64_t>(p[i]);
        hash *= FNV_PRIME;
    }
}

// Store checksum in DB
static int store_checksum(const char* path, uint64_t hash) {
    if (!meta_db) return -EIO;

    std::ostringstream oss;
    oss << std::hex << hash;
    std::string checksum = oss.str();

    const char* sql =
        "INSERT INTO checksums(path, checksum) "
        "VALUES(?, ?) "
        "ON CONFLICT(path) DO UPDATE SET checksum = excluded.checksum;";

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(meta_db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) return -EIO;

    sqlite3_bind_text(stmt, 1, path, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, checksum.c_str(), -1, SQLITE_TRANSIENT);

    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) return -EIO;
    return 0;
}

static std::string full_path(const char* path) {
    std::string result = backing_root;
    if (!result.empty() && result.back() == '/') result.pop_back();
    result += path;
    return result;
}

// Compute raw hash from disk
static uint64_t compute_hash_uint64(const std::string& real_path) {
    int fd = open(real_path.c_str(), O_RDONLY);
    if (fd == -1) return FNV_OFFSET_BASIS;

    uint64_t hash = FNV_OFFSET_BASIS;
    char buf[4096];
    ssize_t n;
    while ((n = read(fd, buf, sizeof(buf))) > 0) {
        update_fnv1a(hash, buf, (size_t)n);
    }
    close(fd);
    return hash;
}

static std::string compute_checksum_for_file(const std::string& real_path) {
    uint64_t hash = compute_hash_uint64(real_path);
    std::ostringstream oss;
    oss << std::hex << hash;
    return oss.str();
}

static bool verify_fd_checksum(const char* path, int fd) {
    if (verified_ok_fds.count(fd)) return true;
    if (verified_bad_fds.count(fd)) return false;
    if (!meta_db) { verified_ok_fds.insert(fd); return true; }

    const char* sql = "SELECT checksum FROM checksums WHERE path = ?;";
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(meta_db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        verified_ok_fds.insert(fd); return true;
    }

    sqlite3_bind_text(stmt, 1, path, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);

    if (rc != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        verified_ok_fds.insert(fd);
        return true;
    }

    const unsigned char* checksum_text = sqlite3_column_text(stmt, 0);
    std::string stored_checksum = checksum_text ? reinterpret_cast<const char*>(checksum_text) : "";
    sqlite3_finalize(stmt);

    if (stored_checksum.empty()) { verified_ok_fds.insert(fd); return true; }

    std::string real = full_path(path);
    std::string current = compute_checksum_for_file(real);

    if (current == stored_checksum) {
        verified_ok_fds.insert(fd);
        return true;
    } else {
        verified_bad_fds.insert(fd);
        return false;
    }
}

// --- FUSE Ops ---

static int fs_getattr(const char* path, struct stat* st) {
    memset(st, 0, sizeof(struct stat));
    std::string real = full_path(path);
    if (lstat(real.c_str(), st) == -1) return -errno;
    return 0;
}

static int fs_readdir(const char* path, void* buf, fuse_fill_dir_t filler,
                      off_t offset, struct fuse_file_info* fi) {
    (void) offset; (void) fi;
    std::string real = full_path(path);
    DIR* dp = opendir(real.c_str());
    if (dp == nullptr) return -errno;
    filler(buf, ".",  nullptr, 0);
    filler(buf, "..", nullptr, 0);
    struct dirent* de;
    while ((de = readdir(dp)) != nullptr) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;
        if (filler(buf, de->d_name, nullptr, 0) != 0) { closedir(dp); return -ENOMEM; }
    }
    closedir(dp);
    return 0;
}

static int fs_open(const char* path, struct fuse_file_info* fi) {
    if (is_append_only_path(path) && (fi->flags & O_TRUNC)) return -EPERM;
    std::string real = full_path(path);
    int fd = open(real.c_str(), fi->flags);
    if (fd == -1) return -errno;
    fi->fh = fd;
    verified_ok_fds.erase(fd);
    verified_bad_fds.erase(fd);
    int accmode = fi->flags & O_ACCMODE;
    if (accmode == O_WRONLY || accmode == O_RDWR) {
        checksum_map[fd] = FNV_OFFSET_BASIS;
    }
    return 0;
}

static int fs_read(const char* path, char* buf, size_t size,
                   off_t offset, struct fuse_file_info* fi) {
    int fd = static_cast<int>(fi->fh);
    if (checksum_map.find(fd) == checksum_map.end()) {
        if (!verify_fd_checksum(path, fd)) return -EIO;
    }
    ssize_t res = pread(fd, buf, size, offset);
    if (res == -1) return -errno;
    return res;
}

static int fs_write(const char* path, const char* buf, size_t size,
                    off_t offset, struct fuse_file_info* fi) {
    int fd = static_cast<int>(fi->fh);
    auto it = checksum_map.find(fd);
    if (it != checksum_map.end()) {
        update_fnv1a(it->second, buf, size);
        
        // --- BAD ARCHITECTURE SIMULATION ---
        // Writing to DB on every 4KB chunk
        store_checksum(path, it->second);
    }
    ssize_t res = pwrite(fd, buf, size, offset);
    if (res == -1) return -errno;
    return res;
}

static int fs_release(const char* path, struct fuse_file_info* fi) {
    int fd = static_cast<int>(fi->fh);
    int res = (close(fd) == -1) ? -errno : 0;
    auto it = checksum_map.find(fd);
    if (it != checksum_map.end()) {
        // In the bad version, we technically already stored it, 
        // but we clean up the map here.
        checksum_map.erase(it);
    }
    verified_ok_fds.erase(fd);
    verified_bad_fds.erase(fd);
    return res;
}

static int fs_create(const char* path, mode_t mode, struct fuse_file_info* fi) {
    std::string real = full_path(path);
    int fd = open(real.c_str(), fi->flags, mode);
    if (fd == -1) return -errno;
    fi->fh = fd;
    int accmode = fi->flags & O_ACCMODE;
    if (accmode == O_WRONLY || accmode == O_RDWR) checksum_map[fd] = FNV_OFFSET_BASIS;
    return 0;
}

static int fs_truncate(const char* path, off_t size) {
    if (is_append_only_path(path)) return -EPERM;
    std::string real = full_path(path);
    if (truncate(real.c_str(), size) == -1) return -errno;
    uint64_t new_hash = compute_hash_uint64(real);
    store_checksum(path, new_hash); 
    return 0;
}

static int fs_unlink(const char* path) {
    if (is_append_only_path(path)) return -EPERM;
    std::string real = full_path(path);
    if (unlink(real.c_str()) == -1) return -errno;
    if (meta_db) {
        sqlite3_exec(meta_db, ("DELETE FROM metadata WHERE path='" + std::string(path) + "'").c_str(), NULL, NULL, NULL);
        sqlite3_exec(meta_db, ("DELETE FROM checksums WHERE path='" + std::string(path) + "'").c_str(), NULL, NULL, NULL);
    }
    return 0;
}

static int fs_mkdir(const char* path, mode_t mode) {
    return mkdir(full_path(path).c_str(), mode) == -1 ? -errno : 0;
}
static int fs_rmdir(const char* path) {
    return rmdir(full_path(path).c_str()) == -1 ? -errno : 0;
}
static int fs_rename(const char* from, const char* to) {
    return rename(full_path(from).c_str(), full_path(to).c_str()) == -1 ? -errno : 0;
}
static int fs_setxattr(const char* path, const char* name, const char* value, size_t size, int flags) {
    return 0; 
}
static int fs_getxattr(const char* path, const char* name, char* value, size_t size) {
    return -ENODATA; 
}
static int fs_listxattr(const char* path, char* list, size_t size) {
    return 0;
}

static struct fuse_operations fs_ops; // Declared, but not initialized

// Function to safely assign members one-by-one
void set_fs_ops() {
    fs_ops = {};  
    fs_ops.getattr = fs_getattr;
    fs_ops.readdir = fs_readdir;
    fs_ops.mkdir = fs_mkdir;
    fs_ops.unlink = fs_unlink;
    fs_ops.rmdir = fs_rmdir;
    fs_ops.rename = fs_rename;
    fs_ops.truncate = fs_truncate;
    fs_ops.open = fs_open;
    fs_ops.read = fs_read;
    fs_ops.write = fs_write;
    fs_ops.release = fs_release;
    fs_ops.create = fs_create;
    fs_ops.setxattr = fs_setxattr;
    fs_ops.getxattr = fs_getxattr;
    fs_ops.listxattr = fs_listxattr;
}

static void* fs_init(struct fuse_conn_info* conn) {
    if (sqlite3_open((backing_root + "/.metadata.db").c_str(), &meta_db) != SQLITE_OK) return nullptr;
    sqlite3_exec(meta_db, "CREATE TABLE IF NOT EXISTS checksums (path TEXT PRIMARY KEY, checksum TEXT);", NULL, NULL, NULL);
    return nullptr;
}
static void fs_destroy(void* private_data) {
    if (meta_db) sqlite3_close(meta_db);
}

static bool is_append_only_path(const char* path) {
    if (append_only_dirs.empty()) return false;
    std::string p(path); 
    for (const auto& dir : append_only_dirs) {
        if (p == dir) return true;
        if (p.size() > dir.size() && p.compare(0, dir.size(), dir) == 0 && p[dir.size()] == '/') return true;
    }
    return false;
}

static void add_append_only_dirs_from_csv(const char* csv) {
    if (!csv) return;
    std::stringstream ss(csv);
    std::string item;
    while (std::getline(ss, item, ',')) {
        if (item.empty()) continue;
        if (item[0] != '/') item = "/" + item; 
        append_only_dirs.push_back(item);
    }
}

static void parse_append_only_option(int& argc, char* argv[]) {
    int i = 2;
    while (i < argc) {
        if (strcmp(argv[i], "-o") == 0 && i + 1 < argc) {
            const char* opt = argv[i + 1];
            const char* key = "append_only_dirs=";
            const char* pos = strstr(opt, key);
            if (pos == opt) {
                const char* csv = opt + strlen(key);
                add_append_only_dirs_from_csv(csv);
                for (int j = i + 1; j < argc - 1; ++j) argv[j] = argv[j + 1];
                --argc;
                for (int j = i; j < argc - 1; ++j) argv[j] = argv[j + 1];
                --argc;
                continue;
            }
        }
        ++i;
    }
}

int main(int argc, char* argv[]) {
    if (argc < 3) return 1;
    backing_root = argv[1];
    parse_append_only_option(argc, argv);
    for (int i = 1; i < argc - 1; ++i) argv[i] = argv[i + 1];
    --argc;
    
    // FIX: Call the setup function instead of static initialization
    set_fs_ops();
    fs_ops.init = fs_init;
    fs_ops.destroy = fs_destroy;
    
    return fuse_main(argc, argv, &fs_ops, NULL);
}