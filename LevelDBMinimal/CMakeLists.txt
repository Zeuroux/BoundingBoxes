cmake_minimum_required(VERSION 3.20)
project(LevelDbMinimal VERSION 1.0 LANGUAGES CXX)

if(NOT CMAKE_SIZEOF_VOID_P EQUAL 8)
    message(FATAL_ERROR "Only x64 supported")
endif()

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(LevelDBMinimal SHARED src/LevelDBMinimal.cpp)

if(MSVC)
    set_property(TARGET LevelDBMinimal PROPERTY MSVC_RUNTIME_LIBRARY "MultiThreaded")
endif()

target_link_libraries(LevelDBMinimal PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/lib/leveldb.lib)

set_target_properties(LevelDBMinimal PROPERTIES
    WINDOWS_EXPORT_ALL_SYMBOLS ON
)