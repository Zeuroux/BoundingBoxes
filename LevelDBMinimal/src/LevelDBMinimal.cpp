#include <cstdint>
#include <cstddef>
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
#include <windows.h>
#include <future> 
#include <atomic>
#include <queue>

#include "leveldb/table.h"
#include "leveldb/env.h"
#include "leveldb/options.h"
#include "leveldb/iterator.h"

#define EXPORT __declspec(dllexport)

using namespace std::literals;

static unsigned int g_threadCount = 0;

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

template <typename Index, typename Func>
void ParallelFor(Index start, Index end, Func&& f) {
    auto count = end - start;
    if (count <= 0) return;
    if (g_threadCount == 0) {
        g_threadCount = std::thread::hardware_concurrency();
        if (g_threadCount == 0) g_threadCount = 2;
    }

    if (count < 32) {
        for (Index i = start; i < end; ++i) f(i);
        return;
    }

    size_t blockSize = count / g_threadCount;
    std::vector<std::future<void>> futures;
    futures.reserve(g_threadCount);

    for (unsigned int t = 0; t < g_threadCount; ++t) {
        Index tStart = start + t * blockSize;
        Index tEnd = (t == g_threadCount - 1) ? end : tStart + blockSize;
        futures.emplace_back(std::async(std::launch::async, [tStart, tEnd, &f]() {
            for (Index i = tStart; i < tEnd; ++i) f(i);
            }));
    }

    for (auto& fut : futures) fut.wait();
}

static inline uint32_t ReadVarint32(const uint8_t* p, size_t& consumed) {
    uint32_t result = 0;
    uint8_t b = *p++; consumed = 1;
    result |= (b & 0x7F); if (!(b & 0x80)) return result;
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

static void CloseSingleLog(MappedLog* log) {
    if (!log) return;
    if (log->data) { UnmapViewOfFile(log->data); log->data = nullptr; }
    if (log->hMap) { CloseHandle(log->hMap); log->hMap = NULL; }
    if (log->hFile != INVALID_HANDLE_VALUE) { CloseHandle(log->hFile); log->hFile = INVALID_HANDLE_VALUE; }
    log->mappedSize = 0;
}

static bool RemapLogIfNeeded(MappedLog* log) {
    if (!log || log->hFile == INVALID_HANDLE_VALUE) return false;
    LARGE_INTEGER sz;
    if (!GetFileSizeEx(log->hFile, &sz)) { CloseSingleLog(log); return false; }
    uint64_t currentOnDiskSize = static_cast<uint64_t>(sz.QuadPart);
    if (currentOnDiskSize == 0) { CloseSingleLog(log); return false; }
    if (currentOnDiskSize == log->mappedSize) return true;

    if (log->data) { UnmapViewOfFile(log->data); if (log->hMap) CloseHandle(log->hMap); log->hMap = NULL; log->data = nullptr; }
    log->hMap = CreateFileMappingA(log->hFile, nullptr, PAGE_READONLY, 0, 0, nullptr);
    if (!log->hMap) return false;
    log->data = (uint8_t*)MapViewOfFile(log->hMap, FILE_MAP_READ, 0, 0, 0);
    if (!log->data) { CloseHandle(log->hMap); log->hMap = NULL; return false; }
    log->mappedSize = currentOnDiskSize; return true;
}

struct IteratorCache {
    std::vector<leveldb::Iterator*> iterators;
    ~IteratorCache() { for (auto* it : iterators) delete it; }
};

static thread_local IteratorCache t_iterCache;

static void EnsureIterators(BedrockDB* db) {
    if (t_iterCache.iterators.size() < db->tables.size()) {
        t_iterCache.iterators.reserve(db->tables.size());
        for (size_t i = t_iterCache.iterators.size(); i < db->tables.size(); ++i) {
            t_iterCache.iterators.push_back(db->tables[i]->table->NewIterator(db->readOptions));
        }
    }
    else if (t_iterCache.iterators.size() > db->tables.size()) {
        for (size_t i = db->tables.size(); i < t_iterCache.iterators.size(); ++i) {
            delete t_iterCache.iterators[i];
        }
        t_iterCache.iterators.resize(db->tables.size());
    }
}

static bool InternalGetToBuffer(BedrockDB* db, const uint8_t* key, size_t keyLen, std::vector<uint8_t>& buffer) {
    EnsureIterators(db);
    leveldb::Slice target(reinterpret_cast<const char*>(key), keyLen);

    for (size_t i = 0; i < db->tables.size(); ++i) {
        leveldb::Iterator* it = t_iterCache.iterators[i];
        if (!it) continue;

        it->Seek(target);
        if (!it->Valid()) continue;

        leveldb::Slice raw = it->key();
        size_t rawSize = raw.size();
        size_t userSize = rawSize > 8 ? rawSize - 8 : rawSize;

        if (userSize == keyLen && memcmp(raw.data(), key, keyLen) == 0) {
            leveldb::Slice v = it->value();
            buffer.assign((const uint8_t*)v.data(), (const uint8_t*)v.data() + v.size());
            return true;
        }
    }
    return false;
}

static bool InternalGetFromSessionToBuffer(LogSession* session, const uint8_t* key, size_t keyLen, std::vector<uint8_t>& buffer) {
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
                const uint8_t* h = p - 1;
                bool validEntry = false;

                while (h >= lookbackStart) {
                    size_t consumed = 0;
                    uint32_t kLen = ReadVarint32(h, consumed);
                    if ((h + consumed) == p && kLen == keyLen) {
                        const uint8_t* valPos = p + keyLen;
                        if (valPos < dataEnd) {
                            size_t consumedVal = 0;
                            uint32_t valLen = ReadVarint32(valPos, consumedVal);
                            if (consumedVal > 0 && consumedVal <= 5) {
                                const uint8_t* valStart = valPos + consumedVal;
                                if (valStart + valLen <= dataEnd) {
                                    buffer.assign(valStart, valStart + valLen);
                                    return true;
                                }
                            }
                        }
                    }
                    h--;
                }
            }
            p++;
        }
    }
    return false;
}

// Iteration Support Structures
typedef void (*DBIterateCallback)(const uint8_t* key, int32_t keyLen, const uint8_t* val, int32_t valLen);

struct IterWrapper {
    leveldb::Iterator* iter;
    size_t tableIndex; // Lower index = Newer table (Higher priority)
    std::string_view userKey;

    IterWrapper(leveldb::Iterator* it, size_t idx) : iter(it), tableIndex(idx) {
        Update();
    }

    void Update() {
        if (iter && iter->Valid()) {
            leveldb::Slice k = iter->key();
            // Internal keys have 8 bytes trailer (SeqNum + Type)
            if (k.size() >= 8) {
                userKey = std::string_view(k.data(), k.size() - 8);
            }
            else {
                userKey = std::string_view(k.data(), k.size());
            }
        }
        else {
            userKey = {};
        }
    }
};

struct IterCompare {
    bool operator()(const IterWrapper* a, const IterWrapper* b) {
        // Min-Heap logic: Return true if a > b.
        // We want smallest user key first.
        int cmp = a->userKey.compare(b->userKey);
        if (cmp != 0) return cmp > 0;

        // If keys are equal, we want the NEWER table (Smaller Index) to come first.
        // In Min-Heap, "Smaller" pops first.
        // So if IndexA > IndexB, A is "Larger" (worse), so return true.
        return a->tableIndex > b->tableIndex;
    }
};

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

        std::stable_sort(db->tables.begin(), db->tables.end(),
            [](auto const& a, auto const& b) { return a->path > b->path; });

        for (size_t i = 0; i < db->tables.size(); ++i)
            db->pathIndex.emplace(db->tables[i]->path, i);

        return db;
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
            foundFiles.emplace_back(entry.path().string(), static_cast<uint64_t>(entry.file_size(ec)));
        }

        for (auto const& [fullPath, size] : foundFiles) {
            auto it = db->pathIndex.find(fullPath);
            if (it == db->pathIndex.end()) {
                auto tbl = LoadTable(fullPath);
                if (tbl) { db->pathIndex.emplace(fullPath, db->tables.size()); db->tables.push_back(std::move(tbl)); changed = true; }
            }
            else {
                size_t idx = it->second;
                if (idx < db->tables.size() && db->tables[idx]->fileSize != size) {
                    auto fresh = LoadTable(fullPath);
                    if (fresh) {
                        delete db->tables[idx]->table; delete db->tables[idx]->file;
                        db->tables[idx] = std::move(fresh); changed = true;
                    }
                }
            }
        }
        if (changed) {
            std::stable_sort(db->tables.begin(), db->tables.end(),
                [](auto const& a, auto const& b) { return a->path > b->path; });
            db->pathIndex.clear();
            for (size_t i = 0; i < db->tables.size(); ++i) db->pathIndex.emplace(db->tables[i]->path, i);
        }
        return changed;
    }

    EXPORT void CloseDB(BedrockDB* db) {
        if (!db) return;
        for (auto& t : db->tables) { if (t) { delete t->table; delete t->file; } }
        delete db;
    }

    EXPORT void IterateDB(
        BedrockDB* db,
        const uint8_t* prefix, int32_t prefixLen,
        const uint8_t* suffix, int32_t suffixLen,
        DBIterateCallback callback
    ) {
        if (!db || !callback) return;

        // 1. Create iterators for all tables
        std::vector<std::unique_ptr<leveldb::Iterator>> ownerVec;
        std::vector<IterWrapper> wrappers;
        ownerVec.reserve(db->tables.size());
        wrappers.reserve(db->tables.size());

        // Create separate iterators to avoid messing with the thread-local cache
        for (size_t i = 0; i < db->tables.size(); ++i) {
            auto* it = db->tables[i]->table->NewIterator(db->readOptions);
            ownerVec.emplace_back(it);

            if (prefix && prefixLen > 0) {
                leveldb::Slice p((const char*)prefix, prefixLen);
                it->Seek(p);
            }
            else {
                it->SeekToFirst();
            }

            if (it->Valid()) {
                wrappers.emplace_back(it, i);
            }
        }

        // 2. Priority Queue for Merging
        std::priority_queue<IterWrapper*, std::vector<IterWrapper*>, IterCompare> pq;
        for (auto& w : wrappers) {
            if (w.iter->Valid()) pq.push(&w);
        }

        std::string lastUserKey;
        bool first = true;

        std::string_view prefixView;
        if (prefix && prefixLen > 0) prefixView = std::string_view((const char*)prefix, prefixLen);

        std::string_view suffixView;
        if (suffix && suffixLen > 0) suffixView = std::string_view((const char*)suffix, suffixLen);

        while (!pq.empty()) {
            IterWrapper* top = pq.top();
            pq.pop();

            // 3. Process Key
            std::string_view currentKey = top->userKey;

            // Check Prefix (optimization: if sorted key doesn't start with prefix, we are done)
            if (!prefixView.empty()) {
                if (currentKey.size() < prefixView.size() ||
                    currentKey.substr(0, prefixView.size()) != prefixView) {
                    // Since keys are sorted, any subsequent key will also not match
                    break;
                }
            }

            // Check Duplicates (Shadowing)
            // Since tables are sorted by age (index 0 = newest), the first time we see a key, it is the newest version.
            bool isNewKey = first || currentKey != lastUserKey;

            if (isNewKey) {
                first = false;
                lastUserKey = std::string(currentKey);

                // Check Type (last byte of internal key usually)
                // We need to look at the internal key to see if it's a deletion.
                // Internal Key: [User Key][Trailer(8b)]
                // Trailer is (Seq << 8) | Type
                // In Little Endian (EncodeFixed64), the first byte of the trailer is the Type.
                leveldb::Slice raw = top->iter->key();
                uint8_t type = 0;

                if (raw.size() >= 8) {
                    // Get the byte at offset (size - 8)
                    type = (uint8_t)raw.data()[raw.size() - 8];
                }

                // kTypeDeletion = 0x0, kTypeValue = 0x1
                // If type is 0 (Deletion), we skip callback (it's deleted).

                if (type == 0x1) {
                    // Check Suffix
                    bool suffixMatch = true;
                    if (!suffixView.empty()) {
                        if (currentKey.size() < suffixView.size() ||
                            currentKey.substr(currentKey.size() - suffixView.size()) != suffixView) {
                            suffixMatch = false;
                        }
                    }

                    if (suffixMatch) {
                        leveldb::Slice v = top->iter->value();
                        callback(
                            (const uint8_t*)currentKey.data(), (int32_t)currentKey.size(),
                            (const uint8_t*)v.data(), (int32_t)v.size()
                        );
                    }
                }
            }

            // 4. Advance Iterator
            top->iter->Next();
            top->Update();
            if (top->iter->Valid()) {
                pq.push(top);
            }
        }
    }

    struct TempResult {
        std::vector<uint8_t> data;
        bool found;
    };

    EXPORT void BatchGetFlat(
        BedrockDB* db,
        const uint8_t* flatKeys,
        const int32_t* keyOffsets,
        const int32_t* keyLengths,
        int32_t count,
        uint8_t** outDataBlock,
        int32_t* outDataOffsets,
        int32_t* outDataLengths,
        uint8_t* outFound
    ) {
        if (!db || count == 0) return;
        std::vector<TempResult> results(count);

        ParallelFor(0, count, [&](int i) {
            results[i].found = InternalGetToBuffer(db, flatKeys + keyOffsets[i], (size_t)keyLengths[i], results[i].data);
            });

        size_t totalSize = 0;
        for (int i = 0; i < count; ++i) {
            if (results[i].found) totalSize += results[i].data.size();
        }

        uint8_t* dataBlock = (uint8_t*)malloc(totalSize > 0 ? totalSize : 1);
        size_t currentOffset = 0;

        for (int i = 0; i < count; ++i) {
            outFound[i] = results[i].found ? 1 : 0;
            if (results[i].found) {
                size_t len = results[i].data.size();
                memcpy(dataBlock + currentOffset, results[i].data.data(), len);
                outDataOffsets[i] = (int32_t)currentOffset;
                outDataLengths[i] = (int32_t)len;
                currentOffset += len;
            }
            else {
                outDataOffsets[i] = 0;
                outDataLengths[i] = 0;
            }
        }
        *outDataBlock = dataBlock;
    }

    EXPORT LogSession* OpenLogSession(const char* dbPath) {
        if (!dbPath) return nullptr;
        std::error_code ec; std::filesystem::path dir(dbPath);
        if (!std::filesystem::exists(dir, ec) || !std::filesystem::is_directory(dir, ec)) return nullptr;
        auto session = new LogSession();
        session->logs.reserve(16);
        for (auto const& entry : std::filesystem::directory_iterator(dir, ec)) {
            if (ec || !entry.is_regular_file()) continue;
            if (!entry.path().filename().string().ends_with(".log")) continue;
            auto log = std::make_unique<MappedLog>();
            log->path = entry.path().string();
            log->hFile = CreateFileA(log->path.c_str(), GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
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

        session->logs.erase(std::remove_if(session->logs.begin(), session->logs.end(), [&](std::unique_ptr<MappedLog>& log) {
            if (!log) return true;
            if (!diskLogs.contains(log->path)) { CloseSingleLog(log.get()); changed = true; return true; }
            if (!RemapLogIfNeeded(log.get())) { CloseSingleLog(log.get()); changed = true; return true; }
            return false;
            }), session->logs.end());

        for (auto const& path : diskLogs) {
            bool already = false;
            for (auto& log : session->logs) if (log->path == path) { already = true; break; }
            if (already) continue;
            auto log = std::make_unique<MappedLog>(); log->path = path;
            log->hFile = CreateFileA(path.c_str(), GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
            if (log->hFile == INVALID_HANDLE_VALUE) continue;
            if (RemapLogIfNeeded(log.get())) { session->logs.push_back(std::move(log)); changed = true; }
            else CloseSingleLog(log.get());
        }
        return changed;
    }

    EXPORT void BatchGetSessionFlat(
        LogSession* session,
        const uint8_t* flatKeys,
        const int32_t* keyOffsets,
        const int32_t* keyLengths,
        int32_t count,
        uint8_t** outDataBlock,
        int32_t* outDataOffsets,
        int32_t* outDataLengths,
        uint8_t* outFound
    ) {
        if (!session || count == 0) return;
        std::vector<TempResult> results(count);

        ParallelFor(0, count, [&](int i) {
            results[i].found = InternalGetFromSessionToBuffer(session, flatKeys + keyOffsets[i], (size_t)keyLengths[i], results[i].data);
            });

        size_t totalSize = 0;
        for (int i = 0; i < count; ++i) if (results[i].found) totalSize += results[i].data.size();

        uint8_t* dataBlock = (uint8_t*)malloc(totalSize > 0 ? totalSize : 1);
        size_t currentOffset = 0;

        for (int i = 0; i < count; ++i) {
            outFound[i] = results[i].found ? 1 : 0;
            if (results[i].found) {
                size_t len = results[i].data.size();
                memcpy(dataBlock + currentOffset, results[i].data.data(), len);
                outDataOffsets[i] = (int32_t)currentOffset;
                outDataLengths[i] = (int32_t)len;
                currentOffset += len;
            }
            else {
                outDataOffsets[i] = 0; outDataLengths[i] = 0;
            }
        }
        *outDataBlock = dataBlock;
    }

    EXPORT void FreeBuffer(uint8_t* buffer) { free(buffer); }
}