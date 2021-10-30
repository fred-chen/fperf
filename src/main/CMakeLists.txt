cmake_minimum_required(VERSION 3.8)
project(agm)

set(CMAKE_C_STANDARD 99)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pedantic -Werror -fno-omit-frame-pointer")
set(CMAKE_VERBOSE_MAKEFILE ON) 

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_executable(fperf fperf.cpp)
if (AGM_RDMA AND NOT (RDMA_INCLUDE_DIRS STREQUAL RDMA_INCLUDE_DIRS-NOTFOUND))
    target_link_libraries(fperf PRIVATE Threads::Threads rdmacm ibverbs)
else()
    target_link_libraries(fperf PRIVATE Threads::Threads)
endif()
