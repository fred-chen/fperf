cmake_minimum_required(VERSION 3.8)
project(fperf)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_COMPILER_VERSION 9)

include_directories(AFTER SYSTEM include)


option(AGM_RDMA "Compile with RDMA support" ON)
FIND_PATH(RDMA_INCLUDE_DIRS rdma/rdma_cma.h)
message ("RDMA_INCLUDE_DIRS: ${RDMA_INCLUDE_DIRS}")

if (RDMA_INCLUDE_DIRS STREQUAL RDMA_INCLUDE_DIRS-NOTFOUND)
  message ("Unable to find rdma includes rdma.h ")
else()
    message ("rdma.h found in ${RDMA_INCLUDE_DIRS}")
    if (AGM_RDMA)
        add_definitions(-DAGM_RDMA)
    endif()
endif()

option(AGM_TEST "Enter test mode" OFF)
if (AGM_TEST)
  add_definitions(-DAGM_TEST)
endif()

option(AGM_CRC "Enter string crc" OFF)
if (AGM_CRC)
  add_definitions(-DAGM_CRC)
endif()

enable_testing()

add_subdirectory("main")
