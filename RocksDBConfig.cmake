
####### Expanded from @PACKAGE_INIT@ by configure_package_config_file() #######
####### Any changes to this file will be overwritten by the next CMake run ####
####### The input file was RocksDBConfig.cmake.in                            ########

get_filename_component(PACKAGE_PREFIX_DIR "${CMAKE_CURRENT_LIST_DIR}/../../../../" ABSOLUTE)

# Use original install prefix when loaded through a "/usr move"
# cross-prefix symbolic link such as /lib -> /usr/lib.
get_filename_component(_realCurr "${CMAKE_CURRENT_LIST_DIR}" REALPATH)
get_filename_component(_realOrig "/usr/lib/x86_64-linux-gnu/cmake/rocksdb" REALPATH)
if(_realCurr STREQUAL _realOrig)
  set(PACKAGE_PREFIX_DIR "/usr")
endif()
unset(_realOrig)
unset(_realCurr)

macro(set_and_check _var _file)
  set(${_var} "${_file}")
  if(NOT EXISTS "${_file}")
    message(FATAL_ERROR "File or directory ${_file} referenced by variable ${_var} does not exist !")
  endif()
endmacro()

macro(check_required_components _NAME)
  foreach(comp ${${_NAME}_FIND_COMPONENTS})
    if(NOT ${_NAME}_${comp}_FOUND)
      if(${_NAME}_FIND_REQUIRED_${comp})
        set(${_NAME}_FOUND FALSE)
      endif()
    endif()
  endforeach()
endmacro()

####################################################################################

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/modules")

include(CMakeFindDependencyMacro)

set(GFLAGS_USE_TARGET_NAMESPACE ON)

if(OFF)
  find_dependency(JeMalloc)
endif()

if(ON)
  find_dependency(gflags CONFIG)
  if(NOT gflags_FOUND)
    find_dependency(gflags)
  endif()
endif()

if(OFF)
  find_dependency(Snappy CONFIG)
  if(NOT Snappy_FOUND)
    find_dependency(Snappy)
  endif()
endif()

if(OFF)
  find_dependency(ZLIB)
endif()

if(OFF)
  find_dependency(BZip2)
endif()

if(OFF)
  find_dependency(lz4)
endif()

if(OFF)
  find_dependency(zstd)
endif()

if(OFF)
  find_dependency(NUMA)
endif()

if(OFF)
  find_dependency(TBB)
endif()

find_dependency(Threads)

include("${CMAKE_CURRENT_LIST_DIR}/RocksDBTargets.cmake")
check_required_components(RocksDB)
