# Copyright 2017 The LevelDB Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

cmake_minimum_required(VERSION 3.9)
# Keep the version below in sync with the one in db.h
project(leveldb VERSION 1.22.0 LANGUAGES C CXX)

add_definitions(-std=c++11)
add_compile_options(-fexceptions)

#set(CMAKE_INCLUDE_CURRENT_DIR ON)
set(CMAKE_CXX_COMPILER /usr/bin/g++)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (WIN32)
    set(LEVELDB_PLATFORM_NAME LEVELDB_PLATFORM_WINDOWS)
    # TODO(cmumford): Make UNICODE configurable for Windows.
    add_definitions(-D_UNICODE -DUNICODE)
else (WIN32)
    set(LEVELDB_PLATFORM_NAME LEVELDB_PLATFORM_POSIX)
endif (WIN32)

option(LEVELDB_BUILD_TESTS "Build LevelDB's unit tests" OFF)
option(LEVELDB_BUILD_BENCHMARKS "Build LevelDB's benchmarks" ON)
option(LEVELDB_INSTALL "Install LevelDB's header and library" ON)

include(TestBigEndian)
test_big_endian(LEVELDB_IS_BIG_ENDIAN)

include(CheckIncludeFile)
check_include_file("unistd.h" HAVE_UNISTD_H)

include(CheckLibraryExists)
check_library_exists(crc32c crc32c_value "" HAVE_CRC32C)
check_library_exists(snappy snappy_compress "" HAVE_SNAPPY)
check_library_exists(tcmalloc malloc "" HAVE_TCMALLOC)

include(CheckCXXSymbolExists)
# Using check_cxx_symbol_exists() instead of check_c_symbol_exists() because
# we're including the header from C++, and feature detection should use the same
# compiler language that the project will use later. Principles aside, some
# versions of do not expose fdatasync() in <unistd.h> in standard C mode
# (-std=c11), but do expose the function in standard C++ mode (-std=c++11).
check_cxx_symbol_exists(fdatasync "unistd.h" HAVE_FDATASYNC)
check_cxx_symbol_exists(F_FULLFSYNC "fcntl.h" HAVE_FULLFSYNC)
check_cxx_symbol_exists(O_CLOEXEC "fcntl.h" HAVE_O_CLOEXEC)

# Test whether -Wthread-safety is available. See
# https://clang.llvm.org/docs/ThreadSafetyAnalysis.html
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag(-Wthread-safety HAVE_CLANG_THREAD_SAFETY)

include(CheckCXXSourceCompiles)

# Test whether C++17 __has_include is available.
check_cxx_source_compiles("
#if defined(__has_include) &&  __has_include(<string>)
#include <string>
#endif
int main() { std::string str; return 0; }
" HAVE_CXX17_HAS_INCLUDE)

set(LEVELDB_PUBLIC_INCLUDE_DIR "include/leveldb")
set(LEVELDB_PORT_CONFIG_DIR "include/port")

configure_file(
        "port/port_config.h.in"
        "${PROJECT_BINARY_DIR}/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
)

include_directories(
        "${PROJECT_BINARY_DIR}/include"
        "."
)

if (BUILD_SHARED_LIBS)
    # Only export LEVELDB_EXPORT symbols from the shared library.
    add_compile_options(-fvisibility=hidden)
endif (BUILD_SHARED_LIBS)

# Must be included before CMAKE_INSTALL_INCLUDEDIR is used.
include(GNUInstallDirs)

add_library(leveldb "" novalsm/lsm_tree_cleaner.cpp novalsm/lsm_tree_cleaner.h db/flush_order.cpp db/flush_order.h)
target_sources(leveldb
        PRIVATE
        "${PROJECT_BINARY_DIR}/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
        include/leveldb/stoc_client.h
        include/leveldb/db_types.h

        novalsm/rdma_admission_ctrl.cpp
        novalsm/rdma_admission_ctrl.h
        db/lookup_index.cpp
        db/lookup_index.h
        stoc/storage_worker.cpp
        stoc/storage_worker.h
        ltc/storage_selector.cpp
        ltc/storage_selector.h
        ltc/stat_thread.cpp
        ltc/stat_thread.h
        db/subrange.cpp
        include/leveldb/subrange.h
        db/compaction.cpp
        db/compaction.h
        db/subrange_manager.cpp
        db/subrange_manager.h
        "db/builder.cc"
        "db/builder.h"
        "db/c.cc"
        "db/db_impl.cc"
        "db/db_impl.h"
        "db/db_iter.cc"
        "db/db_iter.h"
        "db/dbformat.cc"
        "db/dbformat.h"
        "db/filename.cc"
        "db/filename.h"
        "db/log_format.h"
        "db/log_reader.cc"
        "db/log_reader.h"
        "db/log_writer.cc"
        "include/leveldb/log_writer.h"
        "db/memtable.cc"
        "db/memtable.h"
        "db/skiplist.h"
        "db/snapshot.h"
        "db/table_cache.cc"
        "db/table_cache.h"
        "db/version_edit.cc"
        "db/version_edit.h"
        "db/version_set.cc"
        "db/version_set.h"
        "db/write_batch_internal.h"
        "db/write_batch.cc"
        "port/port_stdcxx.h"
        "port/port.h"
        "port/thread_annotations.h"
        "table/block_builder.cc"
        "table/block_builder.h"
        "table/block.cc"
        "table/block.h"
        "table/filter_block.cc"
        "table/filter_block.h"
        "table/format.cc"
        "table/format.h"
        "table/iterator_wrapper.h"
        "table/iterator.cc"
        "table/merger.cc"
        "table/merger.h"
        "table/table_builder.cc"
        "table/table.cc"
        "table/two_level_iterator.cc"
        "table/two_level_iterator.h"
        "util/arena.cc"
        "util/arena.h"
        "util/bloom.cc"
        "util/cache.cc"
        "util/coding.cc"
        "util/coding.h"
        "util/comparator.cc"
        "util/crc32c.cc"
        "util/crc32c.h"
        "util/env.cc"
        "util/filter_policy.cc"
        "util/hash.cc"
        "util/hash.h"
        "util/logging.cc"
        "util/logging.h"
        "util/mutexlock.h"
        "util/no_destructor.h"
        "util/options.cc"
        "util/random.h"
        "util/status.cc"
        "util/db_profiler.cpp"
        "util/env_mem.cc"
        "util/env_mem.h"
        "util/env_posix.h"
        "ltc/stoc_client_impl.cpp"
        "ltc/stoc_client_impl.h"
        "novalsm/rdma_server.cpp"
        "novalsm/rdma_server.h"


        "util/testharness.cc"
        "util/testharness.h"
        "util/testutil.cc"
        "util/testutil.h"

        db/range_index.cpp
        db/range_index.h
        ltc/db_migration.cpp
        ltc/db_migration.h
        log/log_recovery.cpp
        log/log_recovery.h
        ltc/db_helper.cpp
        ltc/db_helper.h

        stoc/persistent_stoc_file.cpp
        stoc/persistent_stoc_file.h
        bench_memtable/memtable_worker.cpp
        bench_memtable/memtable_worker.h
        ltc/compaction_thread.cpp
        ltc/compaction_thread.h
        common/city_hash.h
        common/city_hash.cpp
        rdma/common.hpp
        common/nova_console_logging.h
        rdma/mr.hpp
        rdma/msg_interface.hpp
        rdma/pre_connector.hpp
        rdma/qp.hpp
        rdma/qp_impl.hpp
        rdma/rdma_ctrl.hpp
        rdma/rdma_ctrl_impl.hpp
        rdma/rnic.hpp
        common/nova_config.h
        common/nova_config.cc
        rdma/nova_msg_parser.h
        novalsm/nic_server.cpp
        novalsm/nic_server.h
        novalsm/client_req_worker.cpp
        novalsm/client_req_worker.h
        common/nova_common.cpp
        common/nova_common.h
        rdma/rdma_msg_callback.h
        rdma/nova_rdma_rc_broker.cpp
        rdma/nova_rdma_rc_broker.h
        rdma/nova_rdma_broker.h
        common/nova_mem_manager.h
        common/nova_mem_manager.cpp
        common/nova_chained_hashtable.cpp
        common/nova_chained_hashtable.h

        log/stoc_log_manager.cpp
        log/stoc_log_manager.h
        log/logc_log_writer.cc
        log/logc_log_writer.h
        common/nova_client_sock.cpp
        common/nova_client_sock.h
        novalsm/rdma_msg_handler.cpp
        novalsm/rdma_msg_handler.h
        ltc/stoc_file_client_impl.cpp
        ltc/stoc_file_client_impl.h

        benchmarks/bench_common.h
        benchmarks/mock_rtable.h
        benchmarks/mock_rtable.cpp
        benchmarks/rdma_write_client.h
        benchmarks/rdma_write_client.cpp

        benchmarks/rdma_write_server_worker.cpp
        benchmarks/rdma_write_server_worker.h

        util/generator.h
        util/zipfian_generator.h
        util/uniform_generator.h
        util/counter_generator.h
        util/discrete_generator.h
        util/scrambled_zipfian_generator.h
        util/skewed_latest_generator.h
        util/utils.h

        # Only CMake 3.3+ supports PUBLIC sources in targets exported by "install".
        $<$<VERSION_GREATER:CMAKE_VERSION,3.2>:PUBLIC>
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/c.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/cache.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/comparator.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/db.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/env.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/export.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/filter_policy.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/iterator.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/options.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/slice.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/status.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/table_builder.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/table.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/write_batch.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/env_bg_thread.h"
        )

if (WIN32)
    target_sources(leveldb
            PRIVATE
            "util/env_windows.cc"
            "util/windows_logger.h"
            )
else (WIN32)
    target_sources(leveldb
            PRIVATE
            "util/env_posix.cc"
            "util/posix_logger.h"
            )
endif (WIN32)

target_include_directories(leveldb
        PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        )

set_target_properties(leveldb
        PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})

target_compile_definitions(leveldb
        PRIVATE
        # Used by include/export.h when building shared libraries.
        LEVELDB_COMPILE_LIBRARY
        # Used by port/port.h.
        ${LEVELDB_PLATFORM_NAME}=1
        )
if (NOT HAVE_CXX17_HAS_INCLUDE)
    target_compile_definitions(leveldb
            PRIVATE
            LEVELDB_HAS_PORT_CONFIG_H=1
            )
endif (NOT HAVE_CXX17_HAS_INCLUDE)

if (BUILD_SHARED_LIBS)
    target_compile_definitions(leveldb
            PUBLIC
            # Used by include/export.h.
            LEVELDB_SHARED_LIBRARY
            )
endif (BUILD_SHARED_LIBS)

if (HAVE_CLANG_THREAD_SAFETY)
    target_compile_options(leveldb
            PUBLIC
            -Werror -Wthread-safety)
endif (HAVE_CLANG_THREAD_SAFETY)

if (HAVE_CRC32C)
    target_link_libraries(leveldb crc32c)
endif (HAVE_CRC32C)
if (HAVE_SNAPPY)
    target_link_libraries(leveldb snappy)
endif (HAVE_SNAPPY)
#if (HAVE_TCMALLOC)
#    target_link_libraries(leveldb tcmalloc)
#endif (HAVE_TCMALLOC)

function(find_static_library LIB_NAME OUT)

    if (WIN32 OR MSVC)
        set(CMAKE_FIND_LIBRARY_SUFFIXES ".lib")
    elseif (UNIX)
        set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
    endif()

    find_library(
            FOUND_${LIB_NAME}_STATIC
            ${LIB_NAME}
    )

    if (FOUND_${LIB_NAME}_STATIC)
        get_filename_component(ABS_FILE ${FOUND_${LIB_NAME}_STATIC} ABSOLUTE)
    else()
        message(SEND_ERROR "Unable to find library ${LIB_NAME}")
    endif()

    set(${OUT} ${ABS_FILE} PARENT_SCOPE)

endfunction()

find_static_library(gflags GFLAGS)

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

# Needed by port_stdcxx.h
find_package(Threads REQUIRED)
target_link_libraries(leveldb Threads::Threads -lpthread)
target_link_libraries(leveldb ibverbs event fmt)

add_executable(nova_server_main "novalsm/nova_server_main.cpp")
target_link_libraries(nova_server_main ${GFLAGS} leveldb)

add_executable(file_reader "novalsm/file_reader.cpp")
target_link_libraries(file_reader -lgflags leveldb)

add_executable(nova_server_main_debug "novalsm/nova_server_main.cpp")
target_link_libraries(nova_server_main_debug -lgflags leveldb -pg)

add_executable(nova_subrange_sim "novalsm/nova_subrange_sim_test.cpp")
target_link_libraries(nova_subrange_sim -lgflags leveldb -pg)

add_executable(scatter_bench "benchmarks/scatter_bench.cpp")
target_link_libraries(scatter_bench -lgflags leveldb)

add_executable(memtable_bench "bench_memtable/memtable_bench.cpp")
target_link_libraries(memtable_bench -lgflags leveldb)

add_executable(version_set_test "db/version_set_test.cc")
target_link_libraries(version_set_test -lgflags leveldb)

add_executable(bloom_test "util/bloom_test.cc")
target_link_libraries(bloom_test -lgflags leveldb)

add_executable(filter_block_test "table/filter_block_test.cc")
target_link_libraries(filter_block_test -lgflags leveldb)
