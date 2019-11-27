project(YCSB_C)

cmake_minimum_required(VERSION 2.8)

## use C++11 features
add_definitions(-std=c++11)

set(CMAKE_INCLUDE_CURRENT_DIR ON)
set(CMAKE_CXX_COMPILER /usr/bin/g++)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

## path to ralloc lib, which you need to build manually
#set(RALLOC_LIB "${PROJECT_SOURCE_DIR}/ralloc/libssmalloc.a")

## include paths
#include_directories(ralloc)

#file(GLOB SOURCES "*.hpp" "*.h" "*.cpp")
set(SOURCES
        logging.h
        client.h
        const_generator.h
        core_workload.cc
        core_workload.h
        counter_generator.h
        db.h
        discrete_generator.h
        generator.h
        properties.h
        scrambled_zipfian_generator.h
        skewed_latest_generator.h
        timer.h
        uniform_generator.h
        utils.h
        zipfian_generator.h
        basic_db.h
        db_factory.cc
        db_factory.h
        nova_mem_client_db.cc nova_mem_client_db.h)

add_library(libycsb STATIC ${SOURCES})
set_target_properties(libycsb PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(libycsb -lpthread)

add_executable(ycsb "ycsbc.cc")
target_link_libraries(ycsb libycsb)