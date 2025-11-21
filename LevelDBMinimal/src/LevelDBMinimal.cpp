#include "LevelDBMinimal.h"
#include <cstring>
#include <string>
#include <string_view>
#include <vector>
#include <filesystem>
#include <fstream>
#include <memory>
#include <unordered_set>
#include <unordered_map>
#include <algorithm>
//https://github.com/purple-blog/build-bedrock-leveldb/
#include "leveldb/table.h"
#include "leveldb/env.h"
#include "leveldb/options.h"
#include "leveldb/iterator.h"

#include <windows.h>

using namespace std::literals;

struct SSTable {
    std::string path;
    uint64_t fileSize = 0;
    leveldb::RandomAccessFile* file = nullptr;
    leveldb::Table* table = nullptr;
};

struct BedrockDB {
    std::vector<std::unique_ptr<SSTable>> tables;
    std::unordered_map<std::string, size_t> pathIndex;
    leveldb::ReadOptions readOptions;
};

struct MappedLog {
    std::string path;
    uint64_t mappedSize = 0;
    uint8_t* data = nullptr;
    HANDLE hFile = INVALID_HANDLE_VALUE;
    HANDLE hMap = NULL;
};

struct LogSession {
    std::vector<std::unique_ptr<MappedLog>> logs;
};

static inline uint32_t ReadVarint32(const uint8_t* p, size_t& consumed) {
    uint32_t result = 0; int shift = 0; consumed = 0;
    uint8_t b = *p++; consumed++; result |= (b & 0x7F); if (!(b & 0x80)) return result;
    b = *p++; consumed++; result |= (b & 0x7F) << 7;  if (!(b & 0x80)) return result;
    b = *p++; consumed++; result |= (b & 0x7F) << 14; if (!(b & 0x80)) return result;
    b = *p++; consumed++; result |= (b & 0x7F) << 21; if (!(b & 0x80)) return result;
    b = *p++; consumed++; result |= (b & 0x7F) << 28; return result;
}

static std::unique_ptr<SSTable> LoadTable(const std::string& fullPath) {
    leveldb::Env* env = leveldb::Env::Default();
    leveldb::RandomAccessFile* file = nullptr;
    if (!env->NewRandomAccessFile(fullPath, &file).ok()) return nullptr;

    uint64_t size = 0; std::error_code ec;
    size = static_cast<uint64_t>(std::filesystem::file_size(fullPath, ec));
    if (ec) { delete file; return nullptr; }

    leveldb::Options opts; opts.compression = leveldb::kNoCompression;

    leveldb::Table* table = nullptr;
    if (!leveldb::Table::Open(opts, file, size, &table).ok()) {
        delete file; return nullptr;
    }

    auto t = std::make_unique<SSTable>();
    t->path = fullPath; t->fileSize = size; t->file = file; t->table = table;
    return t;
}

static void RebuildIndex(BedrockDB* db) {
    db->pathIndex.clear();
    db->pathIndex.reserve(db->tables.size());
    for (size_t i = 0; i < db->tables.size(); ++i)
        db->pathIndex.emplace(db->tables[i]->path, i);
}

static void SortNewestFirst(BedrockDB* db) {
    std::stable_sort(db->tables.begin(), db->tables.end(),
        [](auto const& a, auto const& b) { return a->path > b->path; });
    RebuildIndex(db);
}

static void CloseSingleLog(MappedLog* log) {
    if (!log) return;
    if (log->data) {
        UnmapViewOfFile(log->data);
        log->data = nullptr;
    }
    if (log->hMap) CloseHandle(log->hMap), log->hMap = NULL;
    if (log->hFile != INVALID_HANDLE_VALUE) CloseHandle(log->hFile), log->hFile = INVALID_HANDLE_VALUE;
    log->mappedSize = 0;
}

static bool RemapLogIfNeeded(MappedLog* log) {
    if (!log) return false;
    if (log->hFile == INVALID_HANDLE_VALUE) return false;
    LARGE_INTEGER sz;
    if (!GetFileSizeEx(log->hFile, &sz)) { CloseSingleLog(log); return false; }
    uint64_t currentOnDiskSize = static_cast<uint64_t>(sz.QuadPart);
    if (currentOnDiskSize == 0) { CloseSingleLog(log); return false; }
    if (currentOnDiskSize == log->mappedSize) return true;

    if (log->data) {
        UnmapViewOfFile(log->data); if (log->hMap) CloseHandle(log->hMap); log->hMap = NULL;
        log->data = nullptr;
    }

    log->hMap = CreateFileMappingA(log->hFile, nullptr, PAGE_READONLY, 0, 0, nullptr);
    if (!log->hMap) return false;
    log->data = (uint8_t*)MapViewOfFile(log->hMap, FILE_MAP_READ, 0, 0, 0);
    if (!log->data) { CloseHandle(log->hMap); log->hMap = NULL; return false; }
    log->mappedSize = currentOnDiskSize; return true;
}

extern "C" {
    EXPORT BedrockDB* OpenDB(const char* path) {
        if (!path) return nullptr;
        std::error_code ec; std::filesystem::path dir(path);
        if (!std::filesystem::exists(dir, ec) || !std::filesystem::is_directory(dir, ec)) return nullptr;

        auto db = new BedrockDB();
        db->readOptions.fill_cache = true;
        db->readOptions.verify_checksums = false;

        for (auto const& entry : std::filesystem::directory_iterator(dir, ec)) {
            if (ec || !entry.is_regular_file()) continue;
            std::string file = entry.path().string();
            if (!entry.path().filename().string().ends_with(".ldb")) continue;
            auto tbl = LoadTable(file);
            if (tbl) db->tables.push_back(std::move(tbl));
        }

        if (db->tables.empty()) { delete db; return nullptr; }
        SortNewestFirst(db); return db;
    }

    EXPORT bool UpdateDB(BedrockDB* db, const char* path) {
        if (!db || !path) return false;
        std::error_code ec; std::filesystem::path dir(path);
        if (!std::filesystem::exists(dir, ec) || !std::filesystem::is_directory(dir, ec)) return false;

        bool changed = false;
        std::vector<std::pair<std::string, uint64_t>> foundFiles;
        foundFiles.reserve(64);

        for (auto const& entry : std::filesystem::directory_iterator(dir, ec)) {
            if (ec || !entry.is_regular_file()) continue;
            auto fname = entry.path().filename().string();
            if (!fname.ends_with(".ldb")) continue;
            uint64_t size = static_cast<uint64_t>(entry.file_size(ec));
            if (!ec) foundFiles.emplace_back(entry.path().string(), size);
        }

        for (auto const& [fullPath, size] : foundFiles) {
            auto it = db->pathIndex.find(fullPath);
            if (it == db->pathIndex.end()) {
                auto tbl = LoadTable(fullPath);
                if (tbl) { db->pathIndex.emplace(fullPath, db->tables.size()); db->tables.push_back(std::move(tbl)); changed = true; }
            } else {
                size_t idx = it->second;
                if (idx < db->tables.size() && db->tables[idx]->fileSize != size) {
                    auto fresh = LoadTable(fullPath);
                    if (fresh) {
                        db->tables[idx]->table->~Table();
                        delete db->tables[idx]->table;
                        delete db->tables[idx]->file;
                        db->tables[idx] = std::move(fresh);
                        changed = true;
                    }
                }
            }
        }

        if (changed) SortNewestFirst(db);
        return changed;
    }

    EXPORT void CloseDB(BedrockDB* db) {
        if (!db) return;
        for (auto& t : db->tables) {
            if (t) { delete t->table; delete t->file; }
        }
        delete db;
    }

    EXPORT bool GetValue(BedrockDB* db, const uint8_t* key, size_t keyLen, uint8_t** outVal, size_t* outLen) {
        if (!db || !key || !outVal || !outLen) return false;
        leveldb::Slice target(reinterpret_cast<const char*>(key), keyLen);

        for (auto const& tblPtr : db->tables) {
            if (!tblPtr) continue;
            std::unique_ptr<leveldb::Iterator> it(tblPtr->table->NewIterator(db->readOptions));
            it->Seek(target); if (!it->Valid()) continue;

            leveldb::Slice raw = it->key();
            size_t rawSize = raw.size(), userSize = rawSize > 8 ? rawSize - 8 : rawSize;
            if (userSize == keyLen && memcmp(raw.data(), key, keyLen) == 0) {
                leveldb::Slice v = it->value();
                *outLen = v.size(); *outVal = new uint8_t[*outLen];
                memcpy(*outVal, v.data(), *outLen); return true;
            }
        }
        return false;
    }

    EXPORT void IterateDB(BedrockDB* db, IterCallback callback) {
        if (!db || !callback) return;
        std::unordered_set<size_t> seen;

        for (auto const& tblPtr : db->tables) {
            if (!tblPtr) continue;
            std::unique_ptr<leveldb::Iterator> it(tblPtr->table->NewIterator(db->readOptions));

            for (it->SeekToFirst(); it->Valid(); it->Next()) {
                leveldb::Slice raw = it->key();
                size_t rawSize = raw.size(), userSize = rawSize > 8 ? rawSize - 8 : rawSize;
                std::string_view sv(raw.data(), userSize);
                size_t h = std::hash<std::string_view>{}(sv);
                if (seen.contains(h)) continue;
                seen.insert(h);

                leveldb::Slice v = it->value();
                callback(reinterpret_cast<const uint8_t*>(sv.data()), sv.size(),
                    reinterpret_cast<const uint8_t*>(v.data()), v.size());
            }
        }
    }

    EXPORT LogSession* OpenLogSession(const char* dbPath) {
        if (!dbPath) return nullptr;
        std::error_code ec; std::filesystem::path dir(dbPath);
        if (!std::filesystem::exists(dir, ec) || !std::filesystem::is_directory(dir, ec)) return nullptr;

        auto session = new LogSession();

        for (auto const& entry : std::filesystem::directory_iterator(dir, ec)) {
            if (ec || !entry.is_regular_file()) continue;
            if (!entry.path().filename().string().ends_with(".log")) continue;

            auto log = std::make_unique<MappedLog>();
            log->path = entry.path().string();

            log->hFile = CreateFileA(log->path.c_str(), GENERIC_READ,
                FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr,
                OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
            if (log->hFile == INVALID_HANDLE_VALUE) continue;
            if (RemapLogIfNeeded(log.get())) session->logs.push_back(std::move(log));
            else CloseSingleLog(log.get());
        }

        if (session->logs.empty()) { delete session; return nullptr; }
        return session;
    }

    EXPORT void CloseLogSession(LogSession* session) {
        if (!session) return;
        for (auto& log : session->logs) CloseSingleLog(log.get());
        delete session;
    }

    EXPORT bool UpdateLogSession(LogSession* session, const char* logDir) {
        if (!session || !logDir) return false;
        std::error_code ec; std::filesystem::path dir(logDir);
        if (!std::filesystem::exists(dir, ec) || !std::filesystem::is_directory(dir, ec)) return false;

        bool changed = false; std::unordered_set<std::string> diskLogs;

        for (auto const& entry : std::filesystem::directory_iterator(dir, ec)) {
            if (ec || !entry.is_regular_file()) continue;
            if (!entry.path().filename().string().ends_with(".log")) continue;
            diskLogs.insert(entry.path().string());
        }

        session->logs.erase(
            std::remove_if(session->logs.begin(), session->logs.end(),
                [&](std::unique_ptr<MappedLog>& log) {
            if (!log) return true;
            if (!diskLogs.contains(log->path)) {
                CloseSingleLog(log.get()); changed = true; return true;
            }
            if (!RemapLogIfNeeded(log.get())) {
                CloseSingleLog(log.get()); changed = true; return true;
            }
            return false;
        }),
            session->logs.end());

        for (auto const& path : diskLogs) {
            bool already = false;
            for (auto& log : session->logs)
                if (log->path == path) { already = true; break; }
            if (already) continue;

            auto log = std::make_unique<MappedLog>();
            log->path = path;

            log->hFile = CreateFileA(path.c_str(), GENERIC_READ,
                FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr,
                OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
            if (log->hFile == INVALID_HANDLE_VALUE) continue;

            if (RemapLogIfNeeded(log.get())) {
                session->logs.push_back(std::move(log)); changed = true;
            } else CloseSingleLog(log.get());
        }

        return changed;
    }

    EXPORT bool GetValueFromSession(LogSession* session,
        const uint8_t* key, size_t keyLen,
        uint8_t** outVal, size_t* outLen) {
        if (!session || !key || !outVal || !outLen || keyLen == 0) return false;

        const uint8_t firstChar = key[0];

        for (auto& logPtr : session->logs) {
            MappedLog* log = logPtr.get();
            if (!log->data || log->mappedSize == 0) continue;

            const uint8_t* const dataStart = log->data;
            const uint8_t* const dataEnd = dataStart + log->mappedSize;
            const uint8_t* p = dataStart;

            while (p + keyLen <= dataEnd) {
                const void* found = std::memchr(p, firstChar, dataEnd - p);
                if (!found) break;
                p = static_cast<const uint8_t*>(found);
                if (p + keyLen > dataEnd) break;

                if (memcmp(p, key, keyLen) == 0) {
                    const uint8_t* lookbackStart = (p >= dataStart + 5) ? (p - 5) : dataStart;

                    for (const uint8_t* h = p - 1; h >= lookbackStart; --h) {
                        size_t consumed = 0;
                        uint32_t kLen = ReadVarint32(h, consumed);
                        if ((h + consumed) == p && kLen == keyLen) {
                            const uint8_t* valPos = p + keyLen;
                            if (valPos >= dataEnd) break;

                            size_t consumedVal = 0;
                            uint32_t valLen = ReadVarint32(valPos, consumedVal);

                            if (consumedVal > 0 && consumedVal <= 5) {
                                const uint8_t* valStart = valPos + consumedVal;
                                if (valStart + valLen <= dataEnd) {
                                    *outLen = valLen; *outVal = new uint8_t[valLen];
                                    memcpy(*outVal, valStart, valLen);
                                    return true;
                                }
                            }
                        }
                    }
                }
                p++;
            }
        }
        return false;
    }

    EXPORT void FreeBuffer(uint8_t* buffer) { delete[] buffer; }
    EXPORT void FreePtrArray(uint8_t** ptrs) { delete[] ptrs; }
    EXPORT void FreeSizeArray(size_t* sizes) { delete[] sizes; }
}