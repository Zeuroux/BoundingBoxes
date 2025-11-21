#pragma once
#include <cstdint>
#include <cstddef>

#define EXPORT __declspec(dllexport)

extern "C" {
    struct BedrockDB;
    struct LogSession;

    EXPORT BedrockDB* OpenDB(const char* path);

    EXPORT bool UpdateDB(BedrockDB* db, const char* path);

    EXPORT void CloseDB(BedrockDB* db);

    EXPORT bool GetValue(BedrockDB* db,
        const uint8_t* key, size_t keyLen,
        uint8_t** outVal, size_t* outLen);

    typedef void(*IterCallback)(const uint8_t* key, size_t keyLen,
        const uint8_t* val, size_t valLen);

    EXPORT void IterateDB(BedrockDB* db, IterCallback callback);

    EXPORT LogSession* OpenLogSession(const char* dbPath);

    EXPORT void CloseLogSession(LogSession* session);

    EXPORT bool UpdateLogSession(LogSession* session, const char* logDir);

    EXPORT bool GetValueFromSession(LogSession* session,
        const uint8_t* key, size_t keyLen,
        uint8_t** outVal, size_t* outLen);

    EXPORT void FreeBuffer(uint8_t* buffer);
    EXPORT void FreePtrArray(uint8_t** ptrs);
    EXPORT void FreeSizeArray(size_t* sizes);
}